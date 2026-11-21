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

func TestActiveOwnerCanReconcileAmbiguousOpenWithExactEvidence(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := time.Now()
	lease, err := ledger.AcquireLease(ctx, "dev", "owner", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	deadline := now.Add(time.Second)
	opening, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	proof := lifecycle.SessionClosureProof{Target: target, OwnerName: opening.OwnerName, Reference: "operator-audit-2"}
	if err := ledger.ReconcileSessionClosure(ctx, lease, "build", proof, now); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("accepted proof before the attempt expired: %v", err)
	}
	if err := ledger.ReconcileSessionClosure(ctx, lease, "build", proof, deadline); err != nil {
		t.Fatalf("exact evidence failed under the active lease: %v", err)
	}
	if _, err := ledger.BeginSessionOpen(ctx, lease, "build", target, deadline, deadline.Add(time.Second)); err != nil {
		t.Fatalf("open stayed blocked after recovery: %v", err)
	}
}

func TestRecoveryAcceptsExactEvidenceForAnOlderSessionEpoch(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := time.Now()
	first, err := ledger.AcquireLease(ctx, "dev", "first", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	opening, err := ledger.BeginSessionOpen(ctx, first, "build", target, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	second, err := ledger.AcquireLease(ctx, "dev", "second", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err = ledger.AuthorizeTakeover(ctx, second, now, lifecycle.TakeoverProof{
		PreviousOwner: "first", Reference: "operator-fence-1", ProcessStopped: true, EffectsReconciled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	third, err := ledger.AcquireLease(ctx, "dev", "third", now, time.Minute)
	if err != nil || third.PreviousOwner != second.Owner || third.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("third lease = %+v, error = %v", third, err)
	}
	proof := lifecycle.SessionClosureProof{Target: target, OwnerName: opening.OwnerName, Reference: "operator-audit-1"}
	if err := ledger.ReconcileSessionClosure(ctx, third, "build", proof, now); err != nil {
		t.Fatalf("exact session evidence rejected across epochs: %v", err)
	}
}

func TestOwnedSessionCloseRetryRequiresExactIdentityAndExpiredWindow(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	now := time.Now()
	lease, err := ledger.AcquireLease(ctx, "dev", "owner", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 19}
	deadline := now.Add(time.Second)
	opening, err := ledger.BeginSessionOpen(ctx, lease, "build", target, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := ledger.ConfirmSessionOpen(ctx, lease, "build", opening, "remote-id", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	closing, err := ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, opened.SessionID, now, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, opened.SessionID, now, deadline); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("overlapping close retry: %v", err)
	}
	if _, err := ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, "some-other-id", deadline, deadline.Add(time.Second)); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
		t.Fatalf("wrong identity close retry: %v", err)
	}
	retry, err := ledger.BeginSessionClose(ctx, lease, "build", opened.OwnerName, opened.SessionID, deadline, deadline.Add(time.Second))
	if err != nil || retry.SessionID != closing.SessionID || !retry.Deadline.After(closing.Deadline) {
		t.Fatalf("retry intent = %+v, error = %v", retry, err)
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, "build", closing, deadline); !errors.Is(err, lifecycle.ErrSessionChanged) {
		t.Fatalf("stale close attestation: %v", err)
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, "build", retry, deadline); err != nil {
		t.Fatal(err)
	}
}
