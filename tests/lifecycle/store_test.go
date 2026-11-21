package lifecycle_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

type attributes = map[string]types.AttributeValue

type database struct {
	mu          sync.Mutex
	items       map[lifecycle.Key]attributes
	query       func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	beforeWrite func(*dynamodb.TransactWriteItemsInput) error
	lastTxn     *dynamodb.TransactWriteItemsInput
	reads       int
	effectPages int
}

func (d *database) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads++
	if !aws.ToBool(input.ConsistentRead) || aws.ToString(input.TableName) != "state" {
		return nil, errors.New("expected a strong base-table read")
	}
	return &dynamodb.GetItemOutput{Item: d.items[keyOf(input.Key)]}, nil
}

func (d *database) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if d.query != nil {
		return d.query(input)
	}
	return d.queryEffectHistory(input)
}

func (d *database) queryEffectHistory(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	if aws.ToString(input.TableName) != "state" || !aws.ToBool(input.ConsistentRead) || input.IndexName != nil ||
		aws.ToString(input.KeyConditionExpression) != "PK = :pk AND begins_with(SK, :prefix)" ||
		stringValue(input.ExpressionAttributeValues[":prefix"]) != "OP#" || aws.ToInt32(input.Limit) != 32 {
		return nil, errors.New("unexpected query outside the legacy effect migration")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.effectPages++
	partition := stringValue(input.ExpressionAttributeValues[":pk"])
	var after string
	if len(input.ExclusiveStartKey) != 0 {
		after = keyOf(input.ExclusiveStartKey).SK
	}
	var keys []lifecycle.Key
	for key := range d.items {
		if key.PK == partition && strings.HasPrefix(key.SK, "OP#") && key.SK > after {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(a, b lifecycle.Key) int { return strings.Compare(a.SK, b.SK) })
	page := &dynamodb.QueryOutput{}
	for _, key := range keys {
		if len(page.Items) == int(aws.ToInt32(input.Limit)) {
			break
		}
		page.Items = append(page.Items, d.items[key])
		page.LastEvaluatedKey = keyAttributes(key)
	}
	if len(page.Items) < int(aws.ToInt32(input.Limit)) {
		page.LastEvaluatedKey = nil
	}
	return page, nil
}

func (d *database) TransactWriteItems(_ context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastTxn = input
	if d.beforeWrite != nil {
		if err := d.beforeWrite(input); err != nil {
			return nil, err
		}
	}
	for index, tx := range input.TransactItems {
		if tx.ConditionCheck != nil {
			if !d.leaseMatches(tx.ConditionCheck) {
				return nil, cancelledCondition(index, len(input.TransactItems))
			}
			continue
		}
		put := tx.Put
		if put == nil || aws.ToString(put.TableName) != "state" {
			return nil, errors.New("expected a write to the lifecycle table")
		}
		previous := d.items[keyOf(put.Item)]
		var matches bool
		switch aws.ToString(put.ConditionExpression) {
		case "attribute_not_exists(PK)":
			matches = previous == nil
		case "#rev = :rev AND #deployment = :deployment":
			matches = put.ExpressionAttributeNames["#rev"] == "revision" && put.ExpressionAttributeNames["#deployment"] == "deployment" && previous != nil &&
				numberValue(previous["revision"]) == numberValue(put.ExpressionAttributeValues[":rev"]) &&
				stringValue(previous["deployment"]) == stringValue(put.ExpressionAttributeValues[":deployment"])
		default:
			return nil, errors.New("unexpected revision condition")
		}
		if !matches {
			return nil, cancelledCondition(index, len(input.TransactItems))
		}
	}
	for _, tx := range input.TransactItems {
		if tx.Put != nil {
			d.items[keyOf(tx.Put.Item)] = tx.Put.Item
		}
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func cancelledCondition(index, count int) error {
	reasons := make([]types.CancellationReason, count)
	reasons[index].Code = aws.String("ConditionalCheckFailed")
	return &types.TransactionCanceledException{CancellationReasons: reasons}
}

func (d *database) leaseMatches(check *types.ConditionCheck) bool {
	record := d.items[keyOf(check.Key)]
	if record == nil || aws.ToString(check.ConditionExpression) != "#owner = :owner AND #epoch = :epoch AND #phase = :phase AND #until > :now" {
		return false
	}
	values := check.ExpressionAttributeValues
	until, untilErr := strconv.ParseInt(numberValue(record["leaseUntilMillis"]), 10, 64)
	now, nowErr := strconv.ParseInt(numberValue(values[":now"]), 10, 64)
	return untilErr == nil && nowErr == nil && until > now &&
		stringValue(record["leaseOwner"]) == stringValue(values[":owner"]) &&
		numberValue(record["leaseEpoch"]) == numberValue(values[":epoch"]) &&
		stringValue(record["leasePhase"]) == stringValue(values[":phase"])
}

func keyOf(item attributes) lifecycle.Key {
	return lifecycle.Key{PK: stringValue(item["PK"]), SK: stringValue(item["SK"])}
}

func stringValue(value types.AttributeValue) string {
	return value.(*types.AttributeValueMemberS).Value
}

func numberValue(value types.AttributeValue) string {
	return value.(*types.AttributeValueMemberN).Value
}

func keyAttributes(key lifecycle.Key) attributes {
	return attributes{
		"PK": &types.AttributeValueMemberS{Value: key.PK},
		"SK": &types.AttributeValueMemberS{Value: key.SK},
	}
}

func store(t *testing.T) (*lifecycle.Store, *database) {
	t.Helper()
	db := &database{items: make(map[lifecycle.Key]attributes)}
	ledger, err := lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	return ledger, db
}

func record(deployment string, key lifecycle.Key, payload string) lifecycle.Record {
	return lifecycle.Record{Deployment: deployment, Key: key, Payload: json.RawMessage(payload)}
}

func TestOwnershipClaimAndProfileBindingCommitTogether(t *testing.T) {
	ledger, db := store(t)
	claim := lifecycle.ClaimKey("asg:arn:aws:autoscaling:region:account:group")
	profile := lifecycle.Key{PK: "ENV#dev", SK: "PROFILE#linux"}
	change := func(deployment string, key lifecycle.Key) lifecycle.Change {
		return lifecycle.Change{Record: record(deployment, key, `{"runnerProfile":"linux"}`)}
	}
	if err := ledger.Apply(t.Context(), "fixture-token", change("dev", claim), change("dev", profile)); err != nil {
		t.Fatal(err)
	}
	if len(db.lastTxn.TransactItems) != 2 || aws.ToString(db.lastTxn.ClientRequestToken) != "fixture-token" {
		t.Fatal("claim and binding were not sent in the same idempotent transaction")
	}
	second := lifecycle.Key{PK: "ENV#other", SK: "PROFILE#linux"}
	if err := ledger.Apply(t.Context(), "", change("other", second), change("other", claim)); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("double ownership returned %v", err)
	}
	got, err := ledger.Get(t.Context(), second)
	if err != nil || got != nil {
		t.Fatalf("failed ownership claim left a partial binding: %+v, %v", got, err)
	}
	got, err = ledger.Get(t.Context(), claim)
	if err != nil || got == nil || got.Deployment != "dev" || got.Revision != 1 {
		t.Fatalf("initial owner changed: %+v, %v", got, err)
	}
	wrongOwner := change("other", claim)
	wrongOwner.ExpectedRevision = 1
	if err := ledger.Apply(t.Context(), "", wrongOwner); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("cross-deployment claim update returned %v", err)
	}
}

func TestConcurrentUpdatesUseStrongRevisionConditions(t *testing.T) {
	ledger, _ := store(t)
	key := lifecycle.ProfileKey("dev", "linux", "RUNNER#one")
	start := record("dev", key, `{"state":"registering"}`)
	if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: start}); err != nil {
		t.Fatal(err)
	}
	const writers = 12
	results := make(chan error, writers)
	var group sync.WaitGroup
	for i := range writers {
		group.Go(func() {
			update := record("dev", key, fmt.Sprintf(`{"observedWriter":%d}`, i))
			results <- ledger.Apply(t.Context(), "", lifecycle.Change{Record: update, ExpectedRevision: 1})
		})
	}
	group.Wait()
	close(results)
	var succeeded int
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, lifecycle.ErrConflict):
		default:
			t.Fatal(err)
		}
	}
	got, err := ledger.Get(t.Context(), key)
	if err != nil || got == nil || got.Revision != 2 || succeeded != 1 {
		t.Fatalf("expected one winning transition: %+v, %v, success count %d", got, err, succeeded)
	}
	other := record("other", key, `{"state":"busy"}`)
	if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: other, ExpectedRevision: 2}); err == nil || !strings.Contains(err.Error(), "deployment keys") {
		t.Fatalf("cross-deployment update returned %v", err)
	}
}

func TestInboxDeduplicationAndAcquisitionRecovery(t *testing.T) {
	ledger, _ := store(t)
	message := lifecycle.InboxMessage{
		SessionID: "session-one", MessageID: 9, Digest: strings.Repeat("ab", 32), ReceivedAt: time.Unix(123, 0),
	}
	if created, err := ledger.RecordInbox(t.Context(), "dev", "linux", message); err != nil || !created {
		t.Fatalf("receipt was not created: %t, %v", created, err)
	}
	message.ReceivedAt = message.ReceivedAt.Add(time.Minute)
	if created, err := ledger.RecordInbox(t.Context(), "dev", "linux", message); err != nil || created {
		t.Fatalf("known delivery was not deduplicated: %t, %v", created, err)
	}
	changed := message
	changed.Digest = strings.Repeat("cd", 32)
	if _, err := ledger.RecordInbox(t.Context(), "dev", "linux", changed); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("different message under same delivery ID returned %v", err)
	}
	changed.SessionID = "session-two"
	if created, err := ledger.RecordInbox(t.Context(), "dev", "linux", changed); err != nil || !created {
		t.Fatalf("message IDs from different sessions collided: %t, %v", created, err)
	}
	for i, state := range []string{"pending", "uncertain", "accepted", "accounted"} {
		reservation, err := lifecycle.AcquisitionChange("dev", "linux", lifecycle.Acquisition{RequestID: 101, State: state}, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := ledger.Apply(t.Context(), "", reservation); err != nil {
			t.Fatal(err)
		}
		restored, err := ledger.Get(t.Context(), reservation.Record.Key)
		if err != nil || restored == nil {
			t.Fatalf("reservation missing: %+v, %v", restored, err)
		}
		var current lifecycle.Acquisition
		if err := json.Unmarshal(restored.Payload, &current); err != nil || current.RequestID != 101 || current.State != state {
			t.Fatalf("incorrect durable reservation: %+v, %v", current, err)
		}
	}
}

func TestDueQueryVerifiesSparseIndexCandidatesAndFollowsPages(t *testing.T) {
	ledger, db := store(t)
	before := time.Unix(1700000000, 0).UTC()
	key := func(id string) lifecycle.Key { return lifecycle.ProfileKey("dev", "linux", "OP#"+id) }
	old := record("dev", key("stale"), `{"state":"settled"}`)
	early := record("dev", key("early"), `{"state":"planned"}`)
	early.DueAt = before.Add(-2 * time.Minute)
	later := record("dev", key("later"), `{"state":"planned"}`)
	later.DueAt = before.Add(-time.Minute)
	future := record("dev", key("future"), `{"state":"planned"}`)
	future.DueAt = before.Add(time.Minute)
	if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: old}, lifecycle.Change{Record: early}, lifecycle.Change{Record: later}, lifecycle.Change{Record: future}); err != nil {
		t.Fatal(err)
	}
	if _, present := db.items[key("stale")][lifecycle.DuePartition]; present {
		t.Fatal("settled item entered the sparse index")
	}
	if _, present := db.items[key("early")][lifecycle.DueSort]; !present {
		t.Fatal("pending operation did not enter the sparse index")
	}
	var shards []string
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToString(input.IndexName) != lifecycle.DueIndexName || aws.ToBool(input.ConsistentRead) ||
			aws.ToString(input.ProjectionExpression) != "PK, SK" {
			return nil, errors.New("expected a keys-only eventually consistent index discovery")
		}
		shard := stringValue(input.ExpressionAttributeValues[":shard"])
		shards = append(shards, shard)
		switch shard {
		case "ENV#dev#DUE#1":
			if len(input.ExclusiveStartKey) == 0 {
				return &dynamodb.QueryOutput{
					Items: []attributes{keyAttributes(lifecycle.ProfileKey("dev", "linux", "ACQUIRE#1")), keyAttributes(key("stale"))}, LastEvaluatedKey: keyAttributes(key("stale")),
				}, nil
			}
			return &dynamodb.QueryOutput{Items: []attributes{keyAttributes(key("later"))}}, nil
		case "ENV#dev#DUE#3":
			return &dynamodb.QueryOutput{Items: []attributes{keyAttributes(key("future")), keyAttributes(key("early"))}}, nil
		default:
			return &dynamodb.QueryOutput{}, nil
		}
	}
	found, err := ledger.Due(t.Context(), "dev", before, 1)
	if err != nil || len(found) != 1 || found[0].Key != early.Key || db.reads != 4 ||
		!slices.Contains(shards, "ENV#dev#DUE#7") {
		t.Fatalf("unexpected due records: %+v, error %v, reads %d, shards %v", found, err, db.reads, shards)
	}
}

func TestProfilePaginationAndTTLDoesNotHideSettledRecords(t *testing.T) {
	ledger, db := store(t)
	key := lifecycle.ProfileKey("dev", "linux", "JOB#101")
	row := record("dev", key, `{"state":"settled"}`)
	row.ExpiresAt = time.Unix(1500, 0)
	if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: row}); err != nil {
		t.Fatal(err)
	}
	for _, protected := range []lifecycle.Key{
		lifecycle.ClaimKey("asg-protected"), {PK: "ENV#dev", SK: "LEASE"},
	} {
		expiredOwner := record("dev", protected, `{}`)
		expiredOwner.ExpiresAt = row.ExpiresAt
		if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: expiredOwner}); err == nil {
			t.Fatal("assigned a TTL to ownership")
		}
	}
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if !aws.ToBool(input.ConsistentRead) || input.IndexName != nil ||
			stringValue(input.ExpressionAttributeValues[":pk"]) != key.PK {
			return nil, errors.New("expected a strong profile-partition read")
		}
		if len(input.ExclusiveStartKey) == 0 {
			return &dynamodb.QueryOutput{Items: []attributes{db.items[key]}, LastEvaluatedKey: keyAttributes(key)}, nil
		}
		if keyOf(input.ExclusiveStartKey) != key {
			return nil, errors.New("profile cursor lost")
		}
		return &dynamodb.QueryOutput{}, nil
	}
	rows, cursor, err := ledger.Profile(t.Context(), "dev", "linux", lifecycle.Key{}, 1)
	if err != nil || len(rows) != 1 || !rows[0].ExpiresAt.Equal(row.ExpiresAt) || cursor != key {
		t.Fatalf("expired item or cursor lost: %+v, %+v, %v", rows, cursor, err)
	}
	rows, end, err := ledger.Profile(t.Context(), "dev", "linux", cursor, 1)
	if err != nil || len(rows) != 0 || end != (lifecycle.Key{}) {
		t.Fatalf("unexpected terminal page: %+v, %+v, %v", rows, end, err)
	}
}

func TestStoreClassifiesDynamoDBTransactionContention(t *testing.T) {
	ledger, db := store(t)
	providerErr := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{
		{Code: aws.String("TransactionConflict")},
	}}
	db.beforeWrite = func(*dynamodb.TransactWriteItemsInput) error {
		return providerErr
	}
	change := lifecycle.Change{Record: record("dev", lifecycle.ProfileKey("dev", "linux", "DEMAND"), "{}")}
	err := ledger.Apply(t.Context(), "", change)
	if !errors.Is(err, lifecycle.ErrConflict) || !errors.Is(err, providerErr) {
		t.Fatalf("DynamoDB contention produced %v", err)
	}
}
