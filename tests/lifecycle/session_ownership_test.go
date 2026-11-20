package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestSessionOwnershipClaimsExactSetAndInstallsCursorWithReceipt(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := time.Now()
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ledger.AcquireLease(ctx, "other", "other-controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(30 * time.Second)
	target := lifecycle.SessionTarget{Scope: "https://github.com/enterprises/fixture", ScaleSetID: 7}
	intent, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, deadline)
	if err != nil || intent.Phase != lifecycle.SessionOpening || intent.OwnerName == "" {
		t.Fatalf("opening = %+v, error = %v", intent, err)
	}
	if _, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, deadline); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("second open intent in same profile = %v", err)
	}
	if _, err := ledger.BeginSessionOpen(ctx, other, "other-build", target, now, deadline); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("second deployment's claim = %v", err)
	}
	if _, err := ledger.BeginSessionOpen(ctx, lease, "other-build", target, now, deadline); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("second profile's claim = %v", err)
	}
	opened, err := ledger.ConfirmSessionOpen(ctx, lease, "build", intent, "first-session",
		&lifecycle.DemandCounts{Assigned: 4}, now)
	if err != nil || opened.Phase != lifecycle.SessionOpen || opened.SessionID != "first-session" {
		t.Fatalf("opened = %+v, error = %v", opened, err)
	}
	cursor, err := ledger.SessionProgress(ctx, "dev", "build")
	if err != nil || cursor.SessionID != opened.SessionID || cursor.Demand == nil || cursor.Demand.Assigned != 4 {
		t.Fatalf("cursor = %+v, error = %v", cursor, err)
	}
	if _, err := ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, "wrong-session", now, deadline); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("close of mismatched session = %v", err)
	}
	intent, err = ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, opened.SessionID, now, deadline)
	if err != nil || intent.Phase != lifecycle.SessionClosing {
		t.Fatalf("closing = %+v, error = %v", intent, err)
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, "build", intent, now); err != nil {
		t.Fatal(err)
	}
	cursor, err = ledger.SessionProgress(ctx, "dev", "build")
	if err != nil || cursor.SessionID != opened.SessionID || cursor.Demand != nil {
		t.Fatalf("closed cursor = %+v, error = %v", cursor, err)
	}
	next, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, deadline)
	if err != nil || next.Generation != 2 || next.OwnerName == opened.OwnerName || next.PreviousID != opened.SessionID {
		t.Fatalf("replacement intent = %+v, error = %v", next, err)
	}
}

func TestAmbiguousSessionOpenRequiresExactRecoveryEvidence(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := time.Now()
	lease, err := ledger.AcquireLease(ctx, "dev", "former", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	before := now.Add(10 * time.Second)
	intent, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, before)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginSessionOpen(ctx, lease, "build", target, before, before.Add(time.Second)); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("expired intent permitted a retry: %v", err)
	}
	item, err := ledger.Get(ctx, lifecycle.ProfileKey("dev", "build", "SESSION"))
	if err != nil || item == nil || !item.DueAt.Equal(before.Truncate(time.Millisecond)) {
		t.Fatalf("ownership recovery schedule = %+v, error = %v", item, err)
	}
	now = now.Add(time.Minute)
	recovering, err := ledger.AcquireLease(ctx, "dev", "successor", now, time.Minute)
	if err != nil || recovering.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("lease = %+v, error = %v", recovering, err)
	}
	proof := lifecycle.SessionClosureProof{Target: target, OwnerName: "wrong-owner", Reference: "operator-audit-1"}
	if err := ledger.ReconcileSessionClosure(ctx, recovering, "build", proof, now); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("unrelated proof = %v", err)
	}
	proof.OwnerName = intent.OwnerName
	for range 2 {
		if err := ledger.ReconcileSessionClosure(ctx, recovering, "build", proof, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ledger.BeginSessionOpen(ctx, recovering, "build", target, now, now.Add(time.Second)); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("reconciling a session bypassed deployment fencing: %v", err)
	}
	active, err := ledger.AuthorizeTakeover(ctx, recovering, now, lifecycle.TakeoverProof{
		PreviousOwner: "former", Reference: "operator-fence-1", ProcessStopped: true, EffectsReconciled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	next, err := ledger.BeginSessionOpen(ctx, active, "build", target, now, now.Add(time.Second))
	if err != nil || next.Generation != 2 || next.Epoch != active.Epoch {
		t.Fatalf("generation after verified recovery = %+v, error = %v", next, err)
	}
}
