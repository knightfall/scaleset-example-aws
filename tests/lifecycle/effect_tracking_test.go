package lifecycle_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func journalEffect(id string, now time.Time) lifecycle.Effect {
	return lifecycle.Effect{ID: id, Kind: "provision", Target: "resource", RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(30 * time.Second)}
}

func assertEffectCount(t *testing.T, ledger *lifecycle.Store, lease lifecycle.Lease, profile string, want int) {
	t.Helper()
	record, err := ledger.Get(t.Context(), lifecycle.ProfileKey(lease.Deployment, profile, "EFFECTS"))
	if err != nil || record == nil {
		t.Fatalf("read effect count: %+v, %v", record, err)
	}
	var tracking struct {
		Version int   `json:"version"`
		Epoch   int64 `json:"epoch"`
		Count   int   `json:"count"`
	}
	if err := json.Unmarshal(record.Payload, &tracking); err != nil || tracking.Count != want || tracking.Epoch != lease.Epoch || tracking.Version != 1 {
		t.Fatalf("effect tracking = %+v, want count %d, error = %v", tracking, want, err)
	}
	settled, err := ledger.ProfileEffectsSettled(t.Context(), lease.Deployment, profile)
	if err != nil || settled != (want == 0) {
		t.Fatalf("settled = %t, want count %d, error = %v", settled, want, err)
	}
}

func TestEffectTrackingFollowsJournalTransitionsWithoutScanningHistory(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	intent := journalEffect("first", now)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	db.query = func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		return nil, errors.New("initialized tracking must not scan settled history")
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	intent.Deadline = now.Add(40 * time.Second)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	if _, err := ledger.BeginEffect(ctx, lease, "build", intent.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkEffectUncertain(ctx, lease, "build", intent.ID, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	if _, err := ledger.ResolveEffect(ctx, lease, "build", intent.ID, now, true, "provider:confirmed"); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 0)
	intent = journalEffect("cancelled", now)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	if cancelled, err := ledger.CancelPlannedEffect(ctx, lease, "build", intent.ID, now); err != nil || !cancelled {
		t.Fatalf("cancel planned effect = %t, %v", cancelled, err)
	}
	assertEffectCount(t, ledger, lease, "build", 0)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	if _, err := ledger.BeginEffect(ctx, lease, "build", intent.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ResolveEffect(ctx, lease, "build", intent.ID, intent.Deadline, false, "provider:absent"); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 0)
}

func TestEffectTrackingAndJournalCommitAtomically(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("counter transaction interrupted")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, tx := range input.TransactItems {
			if tx.Put != nil && keyOf(tx.Put.Item).SK == "EFFECTS" {
				return interrupted
			}
		}
		return nil
	}
	intent := journalEffect("first", now)
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted plan: %v", err)
	}
	if effect, err := ledger.Effect(ctx, "dev", "build", intent.ID); err != nil || effect != nil {
		t.Fatalf("failed counter write committed an effect: %+v, %v", effect, err)
	}
	db.beforeWrite = nil
	if _, err := ledger.PlanEffect(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, tx := range input.TransactItems {
			if tx.Put != nil && keyOf(tx.Put.Item).SK == "EFFECTS" {
				return interrupted
			}
		}
		return nil
	}
	if _, err := ledger.CancelPlannedEffect(ctx, lease, "build", intent.ID, now); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted settlement: %v", err)
	}
	assertEffectCount(t, ledger, lease, "build", 1)
	if effect, err := ledger.Effect(ctx, "dev", "build", intent.ID); err != nil || effect == nil || effect.State != lifecycle.EffectPlanned {
		t.Fatalf("failed settlement changed the effect: %+v, %v", effect, err)
	}
	db.beforeWrite = nil
	if _, err := ledger.CancelPlannedEffect(ctx, lease, "build", intent.ID, now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, lease, "build", 0)
}

func TestEffectTrackingRebuildsAfterAControllerWithoutTracking(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := admissionTime
	first, err := ledger.AcquireLease(ctx, "dev", "modern", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.InitializeEffectTracking(ctx, first, "build", now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, first, "build", 0)
	if err := ledger.ReleaseLease(ctx, first, now, lifecycle.Quiescence{SessionsClosed: true, EffectsSettled: true}); err != nil {
		t.Fatal(err)
	}
	legacy, err := ledger.AcquireLease(ctx, "dev", "older-controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// An older binary journals an operation without updating EFFECTS.
	intent := journalEffect("legacy", now)
	intent.State, intent.Epoch = lifecycle.EffectExecuting, legacy.Epoch
	payload, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.ApplyFenced(ctx, legacy, now, "", lifecycle.Change{Record: record("dev", lifecycle.ProfileKey("dev", "build", "OP#legacy"), string(payload))}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	recovering, err := ledger.AcquireLease(ctx, "dev", "successor", now, time.Minute)
	if err != nil || recovering.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("successor lease: %+v, %v", recovering, err)
	}
	if settled, err := ledger.ProfileEffectsSettled(ctx, "dev", "build"); err != nil || settled {
		t.Fatalf("stale zero hid the older binary's operation: %t, %v", settled, err)
	}
	if err := ledger.InitializeEffectTracking(ctx, recovering, "build", now); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, recovering, "build", 1)
	if _, err := ledger.ResolveEffect(ctx, recovering, "build", intent.ID, now, true, "provider:recovered"); err != nil {
		t.Fatal(err)
	}
	assertEffectCount(t, ledger, recovering, "build", 0)
}

func TestEffectTrackingRejectsMalformedCounts(t *testing.T) {
	for _, payload := range []string{
		`{"version":1,"epoch":1}`, `{"version":1,"epoch":1,"count":null}`,
		`{"version":1,"epoch":1,"count":-1}`, `{"version":2,"epoch":1,"count":0}`,
	} {
		t.Run(payload, func(t *testing.T) {
			ledger, _ := store(t)
			key := lifecycle.ProfileKey("dev", "build", "EFFECTS")
			if err := ledger.Apply(t.Context(), "", lifecycle.Change{Record: record("dev", key, payload)}); err != nil {
				t.Fatal(err)
			}
			if settled, err := ledger.ProfileEffectsSettled(t.Context(), "dev", "build"); err == nil || settled {
				t.Fatalf("malformed count became a settled profile: %t, %v", settled, err)
			}
		})
	}
}

func TestEffectTrackingMigratesPaginatedHistoryIncludingVisibleExpiredRows(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 205; index++ {
		id := fmt.Sprintf("settled-%03d", index)
		payload := fmt.Sprintf(`{"id":%q,"state":"confirmed","evidence":"provider:confirmed"}`, id)
		history := record("dev", lifecycle.ProfileKey("dev", "build", "OP#"+id), payload)
		history.ExpiresAt = now.Add(-time.Hour)
		if err := ledger.Apply(ctx, "", lifecycle.Change{Record: history}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.Apply(ctx, "", lifecycle.Change{Record: record("dev", lifecycle.ProfileKey("dev", "build", "OP#unfinished"), `{"id":"unfinished","state":"uncertain"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.InitializeEffectTracking(ctx, lease, "build", now); err != nil {
		t.Fatal(err)
	}
	db.query = func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		return nil, errors.New("bounded tracking scanned expired history")
	}
	assertEffectCount(t, ledger, lease, "build", 1)
}

func TestEffectTrackingSerializesConcurrentCountChanges(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := admissionTime
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.InitializeEffectTracking(ctx, lease, "build", now); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	run := func(action func(string) error) {
		t.Helper()
		results := make(chan error, workers)
		for index := 0; index < workers; index++ {
			id := fmt.Sprintf("concurrent-%d", index)
			go func(id string) {
				for attempt := 0; attempt < 2*workers; attempt++ {
					err := action(id)
					if errors.Is(err, lifecycle.ErrConflict) {
						continue
					}
					results <- err
					return
				}
				results <- errors.New("count contention did not converge")
			}(id)
		}
		for index := 0; index < workers; index++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
	}
	run(func(id string) error {
		_, err := ledger.PlanEffect(ctx, lease, "build", journalEffect(id, now), now)
		return err
	})
	assertEffectCount(t, ledger, lease, "build", workers)
	run(func(id string) error {
		cancelled, err := ledger.CancelPlannedEffect(ctx, lease, "build", id, now)
		if err == nil && !cancelled {
			return errors.New("effect was settled without its cancellation")
		}
		return err
	})
	assertEffectCount(t, ledger, lease, "build", 0)
}
