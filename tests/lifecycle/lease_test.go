package lifecycle_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestCleanLeaseHandoffAdvancesEpoch(t *testing.T) {
	ledger, _ := store(t)
	now := time.Unix(1700000000, 0)
	first, err := ledger.AcquireLease(t.Context(), "dev", "process-a", now, time.Minute)
	if err != nil || first.Phase != lifecycle.LeaseActive || first.Epoch != 1 {
		t.Fatalf("first lease = %+v, error = %v", first, err)
	}
	if _, err := ledger.AcquireLease(t.Context(), "dev", "rival", now, time.Minute); !errors.Is(err, lifecycle.ErrLeaseHeld) {
		t.Fatalf("rival acquisition error = %v", err)
	}
	renewed, err := ledger.RenewLease(t.Context(), first, now.Add(20*time.Second), time.Minute)
	if err != nil || !renewed.ValidUntil.After(first.ValidUntil) || renewed.Epoch != first.Epoch {
		t.Fatalf("renewed lease = %+v, error = %v", renewed, err)
	}
	if err := ledger.ReleaseLease(t.Context(), renewed, now, lifecycle.Quiescence{SessionsClosed: true}); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("release with outstanding effects returned %v", err)
	}
	if err := ledger.ReleaseLease(t.Context(), renewed, now.Add(20*time.Second), lifecycle.Quiescence{SessionsClosed: true, EffectsSettled: true}); err != nil {
		t.Fatal(err)
	}
	second, err := ledger.AcquireLease(t.Context(), "dev", "process-b", now.Add(21*time.Second), time.Minute)
	if err != nil || second.Phase != lifecycle.LeaseActive || second.Epoch != 2 {
		t.Fatalf("second lease = %+v, error = %v", second, err)
	}
}

func TestUncleanTakeoverBlocksOldAndRecoveringWriters(t *testing.T) {
	ledger, db := store(t)
	now := time.Unix(1700000000, 0)
	old, err := ledger.AcquireLease(t.Context(), "dev", "old", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	planned := lifecycle.Effect{
		ID: "scale-1", Kind: "set-desired-capacity", Target: "runner-asg",
		RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(45 * time.Second),
	}
	intent, err := ledger.PlanEffect(t.Context(), old, "linux", planned, now)
	if err != nil || intent.State != lifecycle.EffectPlanned || intent.Revision != 1 {
		t.Fatalf("planned effect = %+v, error = %v", intent, err)
	}
	duplicate, err := ledger.PlanEffect(t.Context(), old, "linux", planned, now)
	if err != nil || duplicate.Revision != 1 {
		t.Fatalf("duplicate effect = %+v, error = %v", duplicate, err)
	}
	if _, err := ledger.BeginEffect(t.Context(), old, "linux", planned.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginEffect(t.Context(), old, "linux", planned.ID, now); !errors.Is(err, lifecycle.ErrEffectNotReady) {
		t.Fatalf("repeating an in-flight effect returned %v", err)
	}

	resumption := now.Add(time.Minute + time.Second)
	standby, err := ledger.AcquireLease(t.Context(), "dev", "new", resumption, time.Minute)
	if err != nil || standby.Phase != lifecycle.LeaseRecovering || standby.PreviousOwner != "old" || standby.Epoch != 2 {
		t.Fatalf("standby lease = %+v, error = %v", standby, err)
	}
	if err := ledger.ReleaseLease(t.Context(), standby, resumption, lifecycle.Quiescence{SessionsClosed: true, EffectsSettled: true}); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("recovering lease could conceal an unclean handoff: %v", err)
	}

	write := lifecycle.Change{Record: record("dev", lifecycle.ProfileKey("dev", "linux", "DEMAND"), `{}`)}
	if err := ledger.ApplyFenced(t.Context(), old, now.Add(time.Second), "", write); !errors.Is(err, lifecycle.ErrLeaseLost) {
		t.Fatalf("paused former owner returned %v", err)
	}
	if len(db.lastTxn.TransactItems) != 2 || db.lastTxn.TransactItems[0].ConditionCheck == nil {
		t.Fatal("lease fence was not part of the write transaction")
	}
	if err := ledger.ApplyFenced(t.Context(), standby, resumption, "", write); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("recovering owner write returned %v", err)
	}
	incomplete := lifecycle.TakeoverProof{PreviousOwner: "old", Reference: "incident-1", AWSIsolated: true, EffectsReconciled: true}
	if _, err := ledger.AuthorizeTakeover(t.Context(), standby, resumption, incomplete); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("partial provider isolation returned %v", err)
	}
	if _, err := ledger.ResolveEffect(t.Context(), standby, "linux", planned.ID, resumption, true, "provider-readback-1"); err != nil {
		t.Fatal(err)
	}
	incomplete.GitHubIsolated = true
	active, err := ledger.AuthorizeTakeover(t.Context(), standby, resumption, incomplete)
	if err != nil || active.Phase != lifecycle.LeaseActive || active.ProofRef != "incident-1" {
		t.Fatalf("verified takeover = %+v, error = %v", active, err)
	}
	if err := ledger.ApplyFenced(t.Context(), active, resumption, "", write); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginEffect(t.Context(), active, "linux", planned.ID, resumption); !errors.Is(err, lifecycle.ErrEffectNotReady) {
		t.Fatalf("confirmed effect was started again: %v", err)
	}
}

func TestUncertainEffectRequiresReadbackBeforeResolution(t *testing.T) {
	ledger, _ := store(t)
	now := time.Unix(1700000000, 0)
	lease, err := ledger.AcquireLease(t.Context(), "dev", "old", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	intent := lifecycle.Effect{
		ID: "scale-1", Kind: "run-task", Target: "runner", RequestSHA256: strings.Repeat("ab", 32),
		Deadline: now.Add(30 * time.Second),
	}
	if _, err := ledger.PlanEffect(t.Context(), lease, "linux", intent, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginEffect(t.Context(), lease, "linux", intent.ID, now); err != nil {
		t.Fatal(err)
	}
	if effect, err := ledger.MarkEffectUncertain(t.Context(), lease, "linux", intent.ID, now); err != nil || effect.State != lifecycle.EffectUncertain {
		t.Fatalf("uncertain effect = %+v, error = %v", effect, err)
	}
	if _, err := ledger.ResolveEffect(t.Context(), lease, "linux", intent.ID, now, false, "readback-too-soon"); !errors.Is(err, lifecycle.ErrEffectNotReady) {
		t.Fatalf("resolved possible in-flight effect as absent: %v", err)
	}
	resolved, err := ledger.ResolveEffect(t.Context(), lease, "linux", intent.ID, intent.Deadline, false, "confirmed-absent")
	if err != nil || resolved.State != lifecycle.EffectAbandoned {
		t.Fatalf("expected resolved absence: %+v, %v", resolved, err)
	}
}

func TestRecoveringLeaseCanCancelAnExpiredPlannedEffect(t *testing.T) {
	ledger, db := store(t)
	now := time.Unix(1700000000, 0)
	old, err := ledger.AcquireLease(t.Context(), "dev", "old", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	const profile = "linux"
	intent := lifecycle.Effect{
		ID: "scale-1", Kind: "run-task", Target: "runner",
		RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(30 * time.Second),
	}
	if _, err := ledger.PlanEffect(t.Context(), old, profile, intent, now); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute + time.Second)
	recovering, err := ledger.AcquireLease(t.Context(), "dev", "new", now, time.Minute)
	if err != nil || recovering.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("recovering lease = %+v, error = %v", recovering, err)
	}
	key := lifecycle.ProfileKey("dev", profile, "OP#"+intent.ID)
	// Simulate a stale GSI projection even after the base record is unscheduled.
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		return &dynamodb.QueryOutput{Items: []attributes{keyAttributes(key)}}, nil
	}
	due, err := ledger.Due(t.Context(), "dev", now, 1)
	if err != nil || len(due) != 1 || due[0].Key != key {
		t.Fatalf("due before cancellation = %+v, error = %v", due, err)
	}
	cancelled, err := ledger.CancelPlannedEffect(t.Context(), recovering, profile, intent.ID, now)
	if err != nil || !cancelled {
		t.Fatalf("cancelled = %t, error = %v", cancelled, err)
	}
	due, err = ledger.Due(t.Context(), "dev", now, 1)
	if err != nil || len(due) != 0 {
		t.Fatalf("cancelled intent still due: %+v, error = %v", due, err)
	}
	intent.Deadline = now.Add(30 * time.Second)
	if _, err := ledger.PlanEffect(t.Context(), recovering, profile, intent, now); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("recovering owner replanned effect: %v", err)
	}
}

func TestPlannedEffectCanRenewAttemptDeadlineBeforeDispatch(t *testing.T) {
	ledger, _ := store(t)
	now := time.Unix(1700000000, 0)
	ctx := t.Context()
	lease, err := ledger.AcquireLease(ctx, "dev", "owner", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	effect := lifecycle.Effect{
		ID: "request", Kind: "scale", Target: "asg",
		RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(time.Second),
	}
	initial, err := ledger.PlanEffect(ctx, lease, "linux", effect, now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	effect.Deadline = now.Add(time.Second)
	replanned, err := ledger.PlanEffect(ctx, lease, "linux", effect, now)
	if err != nil || replanned.Revision != initial.Revision+1 || !replanned.Deadline.Equal(effect.Deadline) {
		t.Fatalf("replanned effect = %+v, error = %v", replanned, err)
	}
	if _, err := ledger.BeginEffect(ctx, lease, "linux", effect.ID, now); err != nil {
		t.Fatal(err)
	}
	effect.Deadline = now.Add(5 * time.Second)
	replayed, err := ledger.PlanEffect(ctx, lease, "linux", effect, now)
	if err != nil || replayed.State != lifecycle.EffectExecuting || !replayed.Deadline.Equal(replanned.Deadline) {
		t.Fatalf("dispatched effect changed on replay: %+v, %v", replayed, err)
	}
}
