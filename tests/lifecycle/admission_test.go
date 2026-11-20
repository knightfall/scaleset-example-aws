package lifecycle_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

var admissionTime = time.Date(2026, 11, 21, 1, 0, 0, 0, time.UTC)

func admissionStore(t *testing.T) (*lifecycle.Store, *database, lifecycle.Lease) {
	t.Helper()
	ledger, db := store(t)
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		t.Helper()
		if aws.ToString(input.KeyConditionExpression) != "PK = :pk AND begins_with(SK, :prefix)" ||
			!aws.ToBool(input.ConsistentRead) || aws.ToInt32(input.Limit) != 1 {
			return nil, errors.New("expected a bounded, strongly consistent reservation query")
		}
		pk := stringValue(input.ExpressionAttributeValues[":pk"])
		prefix := stringValue(input.ExpressionAttributeValues[":prefix"])
		db.mu.Lock()
		defer db.mu.Unlock()
		for key, item := range db.items {
			if key.PK == pk && strings.HasPrefix(key.SK, prefix) {
				return &dynamodb.QueryOutput{Items: []attributes{item}}, nil
			}
		}
		return &dynamodb.QueryOutput{}, nil
	}
	lease, err := ledger.AcquireLease(t.Context(), "dev", "controller", admissionTime, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, db, lease
}

func offer(session string, messageID int64, ids ...int64) lifecycle.AdmissionOffer {
	return lifecycle.AdmissionOffer{
		Message: lifecycle.InboxMessage{
			SessionID: session, MessageID: messageID, Digest: strings.Repeat("ab", 32), ReceivedAt: admissionTime,
		},
		RequestIDs: ids, EffectiveMax: 5, Assigned: 1, OutsideAssigned: 1,
	}
}

func expectDecision(t *testing.T, ledger *lifecycle.Store, lease lifecycle.Lease, offered lifecycle.AdmissionOffer, send, finished bool, ids ...int64) {
	t.Helper()
	result, err := ledger.ReserveAdmission(t.Context(), lease, "build", admissionTime.Add(time.Second), offered)
	if err != nil || !slices.Equal(result.RequestIDs, ids) || result.Send != send || result.OutcomeRecorded != finished {
		t.Fatalf("decision = %+v, error = %v, want ids %v, send %t, outcome recorded %t", result, err, ids, send, finished)
	}
}

func acquisitionState(t *testing.T, ledger *lifecycle.Store, id int64) string {
	t.Helper()
	key := lifecycle.ProfileKey("dev", "build", fmt.Sprintf("ACQUIRE#%d", id))
	record, err := ledger.Get(t.Context(), key)
	if err != nil || record == nil {
		t.Fatalf("read acquisition %d: %v", id, err)
	}
	var acquisition lifecycle.Acquisition
	if err := json.Unmarshal(record.Payload, &acquisition); err != nil {
		t.Fatal(err)
	}
	return acquisition.State
}

func TestAdmissionPreservesAcceptedAndUncertainCapacityAcrossReplayAndRestart(t *testing.T) {
	ledger, db, lease := admissionStore(t)
	first := offer("session-1", 1, 10, 10, 11)
	recorded, err := ledger.RecordInbox(t.Context(), "dev", "build", first.Message)
	if err != nil || !recorded {
		t.Fatalf("record delivery receipt = %t, %v", recorded, err)
	}
	expectDecision(t, ledger, lease, first, true, false, 10, 11)
	now := admissionTime.Add(time.Second)
	if err := ledger.RecordAdmissionOutcome(t.Context(), lease, "build", now, first.Message, []int64{10}); err != nil {
		t.Fatal(err)
	}
	if got, want := acquisitionState(t, ledger, 10), "accepted"; got != want {
		t.Fatalf("confirmed request state = %q, want %q", got, want)
	}
	if got, want := acquisitionState(t, ledger, 11), "uncertain"; got != want {
		t.Fatalf("omitted request state = %q, want %q", got, want)
	}
	first.EffectiveMax = 100
	expectDecision(t, ledger, lease, first, false, true, 10, 11)

	restarted, err := lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	second := offer("session-2", 1, 10, 11, 12, 13)
	expectDecision(t, restarted, lease, second, true, false, 12)
	expectDecision(t, restarted, lease, offer("session-2", 2, 13), false, true)
	// A change in the aggregate cannot authorize a release by itself.
	if err := restarted.ReleaseAdmission(t.Context(), lease, "build", now, lifecycle.AcquisitionEvidence{
		RequestID: 10, State: "accounted",
	}); err == nil {
		t.Fatal("released an acquisition without verified request evidence")
	}
	evidence := lifecycle.AcquisitionEvidence{RequestID: 10, State: "accounted", Reference: "assigned:session-2:3:10"}
	for range 2 {
		if err := restarted.ReleaseAdmission(t.Context(), lease, "build", now, evidence); err != nil {
			t.Fatal(err)
		}
	}
	expectDecision(t, restarted, lease, offer("session-2", 4, 10, 11, 13, 14), true, false, 13)
	expectDecision(t, restarted, lease, offer("session-2", 5, 14), false, true)
}

func TestUncertainAdmissionIsNeverSentAgainAndCanBeRecordedForAcknowledgement(t *testing.T) {
	ledger, db, lease := admissionStore(t)
	first := offer("session-1", 1, 10)
	first.EffectiveMax = 3
	expectDecision(t, ledger, lease, first, true, false, 10)
	restarted, err := lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	expectDecision(t, restarted, lease, first, false, false, 10)

	now := admissionTime.Add(time.Second)
	if err := restarted.RecordAdmissionOutcome(t.Context(), lease, "build", now, first.Message, []int64{999}); err == nil {
		t.Fatal("recorded success for a request outside the reservation")
	}
	if err := restarted.RecordAdmissionOutcome(t.Context(), lease, "build", now, first.Message, nil); err != nil {
		t.Fatal(err)
	}
	expectDecision(t, restarted, lease, first, false, true, 10)
	if err := restarted.RecordAdmissionOutcome(t.Context(), lease, "build", now, first.Message, []int64{10}); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("conflicting outcome error = %v", err)
	}
	second := offer("session-2", 1, 11)
	second.EffectiveMax = 3
	expectDecision(t, restarted, lease, second, false, true)
}

func TestAdmissionSerializesCompetingDeliveriesAndFencesFormerOwner(t *testing.T) {
	ledger, _, lease := admissionStore(t)
	expectDecision(t, ledger, lease, offer("session-1", 1), false, true)
	now := admissionTime.Add(time.Second)
	ctx := t.Context()
	type result struct {
		decision lifecycle.AdmissionDecision
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, id := range []int64{10, 11} {
		go func() {
			<-start
			value := offer("session-1", id, id)
			value.EffectiveMax = 3
			decision, err := ledger.ReserveAdmission(ctx, lease, "build", now, value)
			results <- result{decision: decision, err: err}
		}()
	}
	close(start)
	sent := 0
	for range 2 {
		got := <-results
		if got.err != nil && !errors.Is(got.err, lifecycle.ErrConflict) {
			t.Fatal(got.err)
		}
		if got.decision.Send {
			sent++
		}
	}
	if sent != 1 {
		t.Fatalf("admitted %d deliveries for one available slot", sent)
	}
	if err := ledger.ReleaseLease(ctx, lease, now, lifecycle.Quiescence{SessionsClosed: true, EffectsSettled: true}); err != nil {
		t.Fatal(err)
	}
	next, err := ledger.AcquireLease(ctx, "dev", "replacement", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	newOffer := offer("session-2", 1, 12)
	if _, err := ledger.ReserveAdmission(ctx, lease, "build", now, newOffer); !errors.Is(err, lifecycle.ErrLeaseLost) {
		t.Fatalf("former owner returned %v", err)
	}
	expectDecision(t, ledger, next, newOffer, true, false, 12)
}

func TestAdmissionFitsTransactionLimitAndRejectsPreexistingUncountedRows(t *testing.T) {
	t.Run("transaction limit", func(t *testing.T) {
		ledger, db, lease := admissionStore(t)
		ids := make([]int64, 110)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		value := offer("session-1", 1, ids...)
		value.EffectiveMax = 200
		expectDecision(t, ledger, lease, value, true, false, ids[:97]...)
		if len(db.lastTxn.TransactItems) != 100 || db.lastTxn.TransactItems[0].ConditionCheck == nil {
			t.Fatal("full batch did not include the lease and fit the DynamoDB transaction")
		}
	})
	t.Run("previous uncounted acquisition", func(t *testing.T) {
		ledger, _, lease := admissionStore(t)
		record, err := lifecycle.AcquisitionChange("dev", "build", lifecycle.Acquisition{RequestID: 10, State: "pending"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := ledger.Apply(context.Background(), "", record); err != nil {
			t.Fatal(err)
		}
		_, err = ledger.ReserveAdmission(t.Context(), lease, "build", admissionTime, offer("session-1", 1, 11))
		if err == nil || !strings.Contains(err.Error(), "existing acquisitions require reconciliation") {
			t.Fatalf("legacy acquisition should prevent admission, got %v", err)
		}
	})
}
