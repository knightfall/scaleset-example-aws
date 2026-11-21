// Package lifecycle persists runner ownership and reconciliation records in DynamoDB.
package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	DueIndexName = "due"
	DuePartition = "dueShard"
	DueSort      = "dueAtMillis"
	Expiry       = "expiresAt"
	dueShards    = 8
)

var ErrConflict = errors.New("lifecycle record revision conflict")

type DynamoDB interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type Store struct {
	db    DynamoDB
	table string
}

func NewStore(db DynamoDB, table string) (*Store, error) {
	if db == nil || table == "" {
		return nil, errors.New("lifecycle store requires a table and DynamoDB client")
	}
	return &Store{db: db, table: table}, nil
}

type Key struct {
	PK string
	SK string
}

func ProfileKey(deployment, profile, item string) Key {
	return Key{PK: "PROFILE#" + deployment + "#" + profile, SK: item}
}

func ClaimKey(canonicalIdentity string) Key {
	return Key{PK: "CLAIM#" + canonicalIdentity, SK: "OWNER"}
}

type Record struct {
	Key        Key
	Deployment string
	Revision   int64
	// Payload contains sanitized lifecycle facts. Bootstrap credentials use
	// separate handoff items, never these records.
	Payload   json.RawMessage
	DueAt     time.Time
	ExpiresAt time.Time
}

type Change struct {
	Record           Record
	ExpectedRevision int64
}

// Apply commits distinct records atomically. Revision zero expects a missing item.
func (s *Store) Apply(ctx context.Context, token string, changes ...Change) error {
	return s.apply(ctx, token, nil, changes)
}

func (s *Store) apply(ctx context.Context, token string, fence *types.ConditionCheck, changes []Change) error {
	preconditions := 0
	if fence != nil {
		preconditions = 1
	}
	if len(changes) == 0 || len(changes)+preconditions > 100 {
		return errors.New("lifecycle transaction requires 1 to 100 operations")
	}
	if len(token) > 36 {
		return errors.New("lifecycle transaction token exceeds 36 characters")
	}
	items := make([]types.TransactWriteItem, 0, len(changes)+preconditions)
	seen := make(map[Key]bool, len(changes))
	if fence != nil {
		items = append(items, types.TransactWriteItem{ConditionCheck: fence})
		seen[readKey(fence.Key)] = true
	}
	for _, change := range changes {
		if seen[change.Record.Key] {
			return errors.New("lifecycle transaction repeats a record key")
		}
		seen[change.Record.Key] = true
		put, err := change.put(s.table)
		if err != nil {
			return err
		}
		items = append(items, types.TransactWriteItem{Put: put})
	}

	input := &dynamodb.TransactWriteItemsInput{TransactItems: items}
	if token != "" {
		input.ClientRequestToken = aws.String(token)
	}
	if _, err := s.db.TransactWriteItems(ctx, input); err != nil {
		return transactionError(err, fence != nil)
	}
	return nil
}

func (c Change) put(table string) (*types.Put, error) {
	if c.ExpectedRevision < 0 || c.ExpectedRevision == 1<<63-1 {
		return nil, errors.New("invalid expected lifecycle revision")
	}
	c.Record.Revision = c.ExpectedRevision + 1
	item, err := c.Record.item()
	if err != nil {
		return nil, err
	}
	put := &types.Put{TableName: aws.String(table), Item: item}
	if c.ExpectedRevision == 0 {
		put.ConditionExpression = aws.String("attribute_not_exists(PK)")
		return put, nil
	}
	put.ConditionExpression = aws.String("#rev = :rev AND #deployment = :deployment")
	put.ExpressionAttributeNames = map[string]string{"#rev": "revision", "#deployment": "deployment"}
	put.ExpressionAttributeValues = map[string]types.AttributeValue{
		":rev": number(c.ExpectedRevision), ":deployment": text(c.Record.Deployment),
	}
	return put, nil
}

func transactionError(err error, fenced bool) error {
	cancellation, ok := errors.AsType[*types.TransactionCanceledException](err)
	if !ok {
		return fmt.Errorf("commit lifecycle records: %w", err)
	}
	conflicting := false
	for i, reason := range cancellation.CancellationReasons {
		switch aws.ToString(reason.Code) {
		case "ConditionalCheckFailed":
			if fenced && i == 0 {
				return fmt.Errorf("%w: %w", ErrLeaseLost, err)
			}
			conflicting = true
		case "TransactionConflict":
			conflicting = true
		}
	}
	if conflicting {
		return fmt.Errorf("%w: %w", ErrConflict, err)
	}
	return fmt.Errorf("commit lifecycle records: %w", err)
}

// Get reads the base table consistently. A missing record returns nil.
func (s *Store) Get(ctx context.Context, key Key) (*Record, error) {
	if key.PK == "" || key.SK == "" {
		return nil, errors.New("lifecycle record requires both keys")
	}
	result, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table), Key: key.item(), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("read lifecycle record: %w", err)
	}
	if len(result.Item) == 0 {
		return nil, nil
	}
	record, err := readRecord(result.Item)
	if err != nil {
		return nil, err
	}
	if record.Key != key {
		return nil, errors.New("lifecycle record does not match the requested key")
	}
	return &record, nil
}

// Profile reads a page of one profile's base-table records consistently.
func (s *Store) Profile(ctx context.Context, deployment, profile string, after Key, limit int32) ([]Record, Key, error) {
	if deployment == "" || profile == "" || limit <= 0 {
		return nil, Key{}, errors.New("profile query requires deployment, profile and a positive limit")
	}
	pk := ProfileKey(deployment, profile, "").PK
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(limit),
		KeyConditionExpression:    aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": text(pk)},
	}
	if after != (Key{}) {
		if after.PK != pk || after.SK == "" {
			return nil, Key{}, errors.New("profile cursor belongs to another partition")
		}
		input.ExclusiveStartKey = after.item()
	}
	page, err := s.db.Query(ctx, input)
	if err != nil {
		return nil, Key{}, fmt.Errorf("query lifecycle profile: %w", err)
	}
	records := make([]Record, 0, len(page.Items))
	for _, item := range page.Items {
		record, err := readRecord(item)
		if err != nil {
			return nil, Key{}, err
		}
		if record.Key.PK != pk || record.Deployment != deployment {
			return nil, Key{}, errors.New("lifecycle profile query returned a foreign record")
		}
		records = append(records, record)
	}
	return records, readKey(page.LastEvaluatedKey), nil
}

func (k Key) item() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"PK": text(k.PK), "SK": text(k.SK)}
}

func (r Record) item() (map[string]types.AttributeValue, error) {
	if !r.validIdentity() || !json.Valid(r.Payload) {
		return nil, errors.New("lifecycle record requires valid deployment keys and JSON")
	}
	if !r.ExpiresAt.IsZero() && (!r.DueAt.IsZero() || strings.HasPrefix(r.Key.PK, "CLAIM#") || r.Key.SK == "LEASE") {
		return nil, errors.New("scheduled work and ownership records cannot have a TTL")
	}
	item := r.Key.item()
	item["deployment"] = text(r.Deployment)
	item["revision"] = number(r.Revision)
	item["payload"] = text(string(r.Payload))
	if r.Key == leaseKey(r.Deployment) {
		lease, err := decodeLease(r)
		if err != nil {
			return nil, err
		}
		item["leaseOwner"] = text(lease.Owner)
		item["leaseEpoch"] = number(lease.Epoch)
		item["leasePhase"] = text(lease.Phase)
		item["leaseUntilMillis"] = number(lease.ValidUntil.UnixMilli())
	}
	if !r.DueAt.IsZero() {
		digest := sha256.Sum256([]byte(r.Key.PK + "\x00" + r.Key.SK))
		item[DuePartition] = text(duePartitionKey(r.Deployment, int(digest[0])%dueShards))
		item[DueSort] = number(r.DueAt.UnixMilli())
	}
	if !r.ExpiresAt.IsZero() {
		item[Expiry] = number(r.ExpiresAt.Unix())
	}
	return item, nil
}

func (r Record) validIdentity() bool {
	if r.Deployment == "" || strings.Contains(r.Deployment, "#") || r.Key.SK == "" {
		return false
	}
	if remote, ok := strings.CutPrefix(r.Key.PK, "CLAIM#"); ok {
		return remote != "" && r.Key.SK == "OWNER"
	}
	if profile, ok := strings.CutPrefix(r.Key.PK, "PROFILE#"+r.Deployment+"#"); ok {
		return profile != "" && !strings.Contains(profile, "#")
	}
	return r.Key.PK == "ENV#"+r.Deployment
}

func readRecord(item map[string]types.AttributeValue) (Record, error) {
	record := Record{Key: readKey(item), Deployment: stringValue(item["deployment"]), Payload: json.RawMessage(stringValue(item["payload"]))}
	revision, err := intValue(item["revision"])
	if err != nil || revision <= 0 || !record.validIdentity() || !json.Valid(record.Payload) {
		return record, errors.New("invalid lifecycle record returned by DynamoDB")
	}
	record.Revision = revision
	if due := item[DueSort]; due != nil {
		value, err := intValue(due)
		if err != nil {
			return record, fmt.Errorf("decode lifecycle due time: %w", err)
		}
		record.DueAt = time.UnixMilli(value).UTC()
	}
	if expiry := item[Expiry]; expiry != nil {
		value, err := intValue(expiry)
		if err != nil {
			return record, fmt.Errorf("decode lifecycle expiry: %w", err)
		}
		record.ExpiresAt = time.Unix(value, 0).UTC()
	}
	return record, nil
}

func readKey(item map[string]types.AttributeValue) Key {
	return Key{PK: stringValue(item["PK"]), SK: stringValue(item["SK"])}
}

func text(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

func number(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func stringValue(value types.AttributeValue) string {
	if v, ok := value.(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func intValue(value types.AttributeValue) (int64, error) {
	if v, ok := value.(*types.AttributeValueMemberN); ok {
		return strconv.ParseInt(v.Value, 10, 64)
	}
	return 0, errors.New("expected DynamoDB integer")
}
