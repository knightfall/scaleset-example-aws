package lifecycle_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

var catalogTime = time.Date(2026, 11, 21, 1, 0, 0, 0, time.UTC)

func catalogRollout(generation int64, state string) lifecycle.Rollout {
	rollout := lifecycle.Rollout{
		Generation: generation, Digest: strings.Repeat("ab", 32),
		Profiles: []lifecycle.DesiredProfile{
			{ID: "keep", State: lifecycle.ProfileActive, BindingDigest: strings.Repeat("cc", 32)},
		},
	}
	if state != "" {
		rollout.Profiles = append(rollout.Profiles, lifecycle.DesiredProfile{
			ID: "build", State: state, BindingDigest: strings.Repeat("dd", 32),
		})
	}
	return rollout
}

func catalogLease(t *testing.T, ledger *lifecycle.Store, previous lifecycle.Lease, now time.Time) lifecycle.Lease {
	t.Helper()
	ctx := t.Context()
	if previous.Epoch != 0 {
		if err := ledger.ReleaseLease(ctx, previous, now, lifecycle.Quiescence{
			SessionsClosed: true, EffectsSettled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	next, err := ledger.AcquireLease(ctx, "dev", "controller", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func clearPass(id string, at time.Time) lifecycle.RetirementObservation {
	return lifecycle.RetirementObservation{
		PassID: id, ObservedAt: at, ValidUntil: at.Add(time.Minute), GitHubComplete: true, AWSComplete: true,
	}
}

func TestProfileCatalogRequiresDrainProofAndRejectsRollback(t *testing.T) {
	for _, retiredState := range []string{lifecycle.ProfileDisabled, ""} {
		t.Run("retired-"+retiredState, func(t *testing.T) {
			ctx := t.Context()
			ledger, db := store(t)
			useProfilePrefixQueries(db)
			now := catalogTime
			lease := catalogLease(t, ledger, lifecycle.Lease{}, now)

			initial := catalogRollout(1, lifecycle.ProfileActive)
			if _, err := ledger.ApplyRollout(ctx, lease, initial, now); err != nil {
				t.Fatal(err)
			}
			draining := catalogRollout(2, lifecycle.ProfileDraining)
			draining.Digest = strings.Repeat("ce", 32)
			if _, err := ledger.ApplyRollout(ctx, lease, draining, now); !errors.Is(err, lifecycle.ErrConfigurationLease) {
				t.Fatalf("configuration changed with live epoch: %v", err)
			}
			lease = catalogLease(t, ledger, lease, now)
			retire := catalogRollout(3, retiredState)
			if _, err := ledger.ApplyRollout(ctx, lease, retire, now); !errors.Is(err, lifecycle.ErrProfileRetirement) {
				t.Fatalf("active profile removed without draining: %v", err)
			}
			if _, err := ledger.ApplyRollout(ctx, lease, draining, now); err != nil {
				t.Fatal(err)
			}

			first := clearPass("first", now)
			progress, err := ledger.RecordRetirementObservation(ctx, lease, 2, "build", first, 10*time.Second, now)
			if err != nil || progress.Ready {
				t.Fatalf("first pass = %+v, error = %v", progress, err)
			}
			midpoint := now.Add(5 * time.Second)
			busy := clearPass("busy", midpoint)
			busy.Handoffs = 1
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", busy, 10*time.Second, midpoint)
			if err != nil || progress.Ready || progress.FirstPass != "" {
				t.Fatalf("pending handoff did not reset proof: %+v, %v", progress, err)
			}

			after := now.Add(10 * time.Second)
			first = clearPass("clear-again", after)
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", first, 10*time.Second, after)
			if err != nil || progress.Ready {
				t.Fatalf("one clear pass after reset = %+v, error = %v", progress, err)
			}
			beforeSeparation := now.Add(15 * time.Second)
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", clearPass("too-soon", beforeSeparation), 10*time.Second, beforeSeparation)
			if err != nil || progress.Ready {
				t.Fatalf("close observations did not remain gated: %+v, %v", progress, err)
			}
			after = now.Add(20 * time.Second)
			second := clearPass("second", after)
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", second, 10*time.Second, after)
			if err != nil || !progress.Ready || progress.FirstPass != "clear-again" {
				t.Fatalf("separated clear passes = %+v, error = %v", progress, err)
			}
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", busy, 10*time.Second, after)
			if err != nil || !progress.Ready || progress.Last.PassID != second.PassID {
				t.Fatalf("delayed observation regressed proof: %+v, %v", progress, err)
			}
			late := clearPass("late-busy", now.Add(17*time.Second))
			late.Busy = 1
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", late, 10*time.Second, after)
			if err != nil || progress.Ready || progress.FirstPass != second.PassID {
				t.Fatalf("late activity inside proof window did not reset proof: %+v, %v", progress, err)
			}
			after = now.Add(30 * time.Second)
			progress, err = ledger.RecordRetirementObservation(ctx, lease, 2, "build", clearPass("clear-after-late-activity", after), 10*time.Second, after)
			if err != nil || !progress.Ready {
				t.Fatalf("separated clear passes after late activity = %+v, %v", progress, err)
			}
			lease = catalogLease(t, ledger, lease, after)
			catalog, err := ledger.ApplyRollout(ctx, lease, retire, after)
			if err != nil || catalog.Profiles["build"].State != lifecycle.ProfileRetired ||
				catalog.Profiles["keep"].State != lifecycle.ProfileActive {
				t.Fatalf("retirement result = %+v, error = %v", catalog, err)
			}

			if _, err := ledger.ApplyRollout(ctx, lease, draining, after); !errors.Is(err, lifecycle.ErrConfigurationGeneration) {
				t.Fatalf("old generation after retirement = %v", err)
			}
			lease = catalogLease(t, ledger, lease, after)
			if _, err := ledger.ApplyRollout(ctx, lease, catalogRollout(4, lifecycle.ProfileActive), after); !errors.Is(err, lifecycle.ErrProfileMigration) {
				t.Fatalf("retired ID was reused: %v", err)
			}
		})
	}
}

func TestProfileCatalogChecksLedgerBeforeAcceptingRetirement(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	useProfilePrefixQueries(db)
	now := catalogTime
	lease := catalogLease(t, ledger, lifecycle.Lease{}, now)
	if _, err := ledger.ApplyRollout(ctx, lease, catalogRollout(1, lifecycle.ProfileDraining), now); err != nil {
		t.Fatal(err)
	}
	for _, pass := range []lifecycle.RetirementObservation{clearPass("first", now), clearPass("second", now.Add(time.Second))} {
		if _, err := ledger.RecordRetirementObservation(ctx, lease, 1, "build", pass, time.Second, pass.ObservedAt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ledger.PlanEffect(ctx, lease, "build", lifecycle.Effect{
		ID: "unfinished", Kind: "handoff", RequestSHA256: strings.Repeat("cc", 32),
		Target: "runner", Deadline: now.Add(time.Minute),
	}, now); err != nil {
		t.Fatal(err)
	}
	lease = catalogLease(t, ledger, lease, now.Add(2*time.Second))
	if _, err := ledger.ApplyRollout(ctx, lease, catalogRollout(2, ""), now.Add(2*time.Second)); !errors.Is(err, lifecycle.ErrProfileRetirement) {
		t.Fatalf("retirement ignored unsettled durable effect: %v", err)
	}
	catalog, err := ledger.Catalog(ctx, "dev")
	if err != nil || catalog.Generation != 1 || catalog.Profiles["build"].State != lifecycle.ProfileDraining {
		t.Fatalf("failed rollout changed catalog = %+v, error = %v", catalog, err)
	}
}

func TestProfileCatalogRejectsAnAllDisabledRollout(t *testing.T) {
	ledger, _ := store(t)
	now := catalogTime
	lease := catalogLease(t, ledger, lifecycle.Lease{}, now)
	rollout := catalogRollout(1, lifecycle.ProfileDisabled)
	rollout.Profiles = rollout.Profiles[1:]
	if _, err := ledger.ApplyRollout(t.Context(), lease, rollout, now); err == nil {
		t.Fatal("accepted a rollout without enabled profiles")
	}
	catalog, err := ledger.Catalog(t.Context(), "dev")
	if err != nil || catalog.Generation != 0 {
		t.Fatalf("invalid rollout changed catalog = %+v, error = %v", catalog, err)
	}
}

func TestUnrelatedRolloutPreservesDrainProofAndPendingDeliveryBlocksRetirement(t *testing.T) {
	ctx := t.Context()
	ledger, db := store(t)
	useProfilePrefixQueries(db)
	now := catalogTime
	lease := catalogLease(t, ledger, lifecycle.Lease{}, now)
	if _, err := ledger.ApplyRollout(ctx, lease, catalogRollout(1, lifecycle.ProfileDraining), now); err != nil {
		t.Fatal(err)
	}
	for _, pass := range []lifecycle.RetirementObservation{clearPass("first", now), clearPass("second", now.Add(time.Second))} {
		if _, err := ledger.RecordRetirementObservation(ctx, lease, 1, "build", pass, time.Second, pass.ObservedAt); err != nil {
			t.Fatal(err)
		}
	}
	lease = catalogLease(t, ledger, lease, now.Add(2*time.Second))
	updated, err := ledger.ApplyRollout(ctx, lease, catalogRollout(2, lifecycle.ProfileDraining), now.Add(2*time.Second))
	if err != nil || updated.Profiles["build"].Retirement == nil || !updated.Profiles["build"].Retirement.Ready {
		t.Fatalf("unchanged draining profile lost proof: %+v, %v", updated, err)
	}
	if err := ledger.BeginDeliverySession(ctx, lease, "build", "", "owned", nil, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, "build", "owned", now, lifecycle.Delivery{MessageID: 0}); err != nil {
		t.Fatal(err)
	}
	lease = catalogLease(t, ledger, lease, now.Add(3*time.Second))
	if _, err := ledger.ApplyRollout(ctx, lease, catalogRollout(3, ""), now.Add(3*time.Second)); !errors.Is(err, lifecycle.ErrProfileRetirement) {
		t.Fatalf("retirement ignored pending message zero: %v", err)
	}
}
