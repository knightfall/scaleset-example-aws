package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestProfileResourceClaimPreventsSharedTableDoubleOwnership(t *testing.T) {
	ledger, _ := store(t)
	now := time.Now()
	ctx := t.Context()
	first, err := ledger.AcquireLease(ctx, "environment-a", "controller-a", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.AcquireLease(ctx, "environment-b", "controller-b", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	const identity = "github-set-name:https://github.com/example#19#linux"
	if err := ledger.ClaimProfileResource(ctx, first, "build", identity, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ClaimProfileResource(ctx, second, "build", identity, now); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("same target acquired under another environment: %v", err)
	}
	if err := ledger.ClaimProfileResource(ctx, first, "other", identity, now); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("same target acquired under another profile: %v", err)
	}
	if err := ledger.ClaimProfileResource(ctx, first, "build", identity, now); err != nil {
		t.Fatalf("existing owner did not retain claim: %v", err)
	}
}
