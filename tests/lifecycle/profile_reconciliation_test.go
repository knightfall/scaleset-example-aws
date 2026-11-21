package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/reconciler"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type inventorySource func(context.Context, runtimeconfig.Profile) (reconciler.Snapshot, error)

func (f inventorySource) Inspect(ctx context.Context, profile runtimeconfig.Profile) (reconciler.Snapshot, error) {
	return f(ctx, profile)
}

type reconciliationFixture struct {
	ledger      *lifecycle.Store
	db          *database
	lease       lifecycle.Lease
	profile     runtimeconfig.Profile
	now         time.Time
	snapshot    reconciler.Snapshot
	coordinator reconciler.Coordinator
	inspections int
}

func newReconciliationFixture(t *testing.T, state string) *reconciliationFixture {
	t.Helper()
	f := &reconciliationFixture{now: time.Now().UTC()}
	ledger, db, lease := admissionStore(t, f.now)
	useProfilePrefixQueries(db)
	f.ledger, f.lease = ledger, lease
	f.db = db
	f.profile = runtimeconfig.Profile{
		ID: "build", State: state,
		Capacity: runtimeconfig.Capacity{MaxRunners: 5, IdleReserve: 1, StartupTimeoutSeconds: 300},
	}
	if _, err := ledger.ApplyRollout(t.Context(), lease, catalogRollout(1, state), f.now); err != nil {
		t.Fatal(err)
	}
	f.snapshot = reconciler.Snapshot{ObservedAt: f.now, Reference: "inventory:1", Complete: true, BackendLimit: new(5)}
	f.coordinator = reconciler.Coordinator{
		Store: ledger, Generation: 1, ConfigurationDigest: catalogRollout(1, state).Digest, MaxObservationAge: time.Minute, ExitGrace: time.Second,
		Now: func() time.Time { return f.now },
		Provider: inventorySource(func(_ context.Context, profile runtimeconfig.Profile) (reconciler.Snapshot, error) {
			if profile.ID != f.profile.ID {
				return reconciler.Snapshot{}, errors.New("unexpected profile")
			}
			f.inspections++
			return f.snapshot, nil
		}),
	}
	return f
}

func (f *reconciliationFixture) openSession(t *testing.T, counts *lifecycle.DemandCounts) {
	t.Helper()
	ctx := t.Context()
	opening, err := f.ledger.BeginSessionOpen(ctx, f.lease, f.profile.ID,
		lifecycle.SessionTarget{Scope: "https://github.com/example", ScaleSetID: 7}, f.now, f.now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ledger.ConfirmSessionOpen(ctx, f.lease, f.profile.ID, opening, "owned", counts, f.now); err != nil {
		t.Fatal(err)
	}
}

func (f *reconciliationFixture) bind(t *testing.T, id string, runnerID int) lifecycle.ExecutorBinding {
	t.Helper()
	binding := lifecycle.ExecutorBinding{AllocationID: id, ResourceID: "resource-" + id, Incarnation: "boot-" + id}
	if err := f.ledger.BindExecutor(t.Context(), f.lease, f.profile.ID, binding, f.now); err != nil {
		t.Fatal(err)
	}
	if runnerID > 0 {
		if err := f.ledger.BindExecutorRunner(t.Context(), f.lease, f.profile.ID, binding,
			lifecycle.RunnerIdentity{ID: runnerID, Name: fmt.Sprintf("runner-%d", runnerID)}, f.now); err != nil {
			t.Fatal(err)
		}
	}
	return binding
}

func (f *reconciliationFixture) resource(binding lifecycle.ExecutorBinding, state string) reconciler.ObservedResource {
	return reconciler.ObservedResource{ResourceID: binding.ResourceID, Incarnation: binding.Incarnation, Observation: lifecycle.ResourceObservation{
		State: state, ObservedAt: f.now, StartedAt: f.now.Add(-30 * time.Second), Reference: f.snapshot.Reference,
	}}
}

func TestProfileReconciliationCombinesOwnedMissingAndForeignCapacity(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileActive)
	f.openSession(t, &lifecycle.DemandCounts{Assigned: 3, Running: 1})
	busy := f.bind(t, "busy", 7)
	completed := f.bind(t, "completed", 8)
	missing := f.bind(t, "missing", 9)
	starting := f.bind(t, "starting", 0)
	if err := f.ledger.ObserveResource(t.Context(), f.lease, "build", missing,
		f.resource(missing, lifecycle.ResourceRunning).Observation, f.now); err != nil {
		t.Fatal(err)
	}
	if err := f.ledger.ObserveProcess(t.Context(), f.lease, "build", completed, lifecycle.ProcessObservation{
		Runner: lifecycle.RunnerIdentity{ID: 8, Name: "runner-8"}, State: lifecycle.ProcessExited,
		ObservedAt: f.now.Add(-10 * time.Second), Reference: "bootstrap:completed",
	}, f.now); err != nil {
		t.Fatal(err)
	}
	if err := f.ledger.PersistDelivery(t.Context(), f.lease, "build", "owned", f.now, lifecycle.Delivery{
		MessageID: 1, Jobs: []lifecycle.JobFact{
			{RequestID: 100, RunnerID: 7, Phase: lifecycle.JobStarted},
			{RequestID: 101, RunnerID: 8, Phase: lifecycle.JobCompleted},
		},
	}); err != nil {
		t.Fatal(err)
	}
	f.snapshot.Resources = []reconciler.ObservedResource{
		f.resource(busy, lifecycle.ResourceRunning), f.resource(completed, lifecycle.ResourceRunning),
		f.resource(starting, lifecycle.ResourceStarting), {ResourceID: "foreign"},
	}
	result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil {
		t.Fatal(err)
	}
	if result.Serving == nil || *result.Serving != 1 || *result.Busy != 1 || *result.CrediblePending != 1 ||
		*result.Unknown != 2 || *result.Occupied != 5 || *result.Headroom != 0 || result.ProposedStarts != 0 {
		t.Fatalf("incorrect capacity union: %+v", result)
	}
	if !slices.Equal(result.CleanupCandidates, []string{completed.ResourceID}) || !slices.Equal(result.OrphanCandidates, []string{"foreign"}) {
		t.Fatalf("unsafe cleanup or orphan identification: %+v", result)
	}
	stored := readTestExecutor(t, f.ledger, f.lease, "build", missing)
	if stored.Resource.State != lifecycle.ResourceUnknown || stored.ResourceLiveAt.IsZero() {
		t.Fatalf("missing membership erased ownership or credited prior presence: %+v", stored)
	}
}

func TestProfileReconciliationQuarantinesConflictingBindingsAcrossSnapshots(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileActive)
	f.openSession(t, &lifecycle.DemandCounts{})
	binding := f.bind(t, "owned", 7)
	foreign := binding
	foreign.Incarnation = "another-boot"
	f.snapshot.Resources = []reconciler.ObservedResource{f.resource(foreign, lifecycle.ResourceRunning)}
	result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || result.Unknown == nil || *result.Unknown != 1 || *result.Occupied != 1 ||
		!slices.Equal(result.OrphanCandidates, []string{binding.ResourceID}) || len(result.CleanupCandidates) != 0 {
		t.Fatalf("conflicting resource was adopted or counted twice: %+v, %v", result, err)
	}
	stored := readTestExecutor(t, f.ledger, f.lease, "build", binding)
	if !stored.Conflicted || stored.ConflictReference != f.snapshot.Reference {
		t.Fatalf("identity conflict was not retained: %+v", stored)
	}
	f.snapshot.Resources = []reconciler.ObservedResource{f.resource(binding, lifecycle.ResourceRunning)}
	result, err = f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || result.Unknown == nil || *result.Unknown != 1 || len(result.CleanupCandidates) != 0 {
		t.Fatalf("later inventory cleared the conflict: %+v, %v", result, err)
	}
}

func TestProfileReconciliationRequiresFreshCompleteInventoryAndCurrentDemand(t *testing.T) {
	for _, name := range []string{"partial", "stale", "no session", "unknown backend limit", "zero backend limit"} {
		t.Run(name, func(t *testing.T) {
			f := newReconciliationFixture(t, lifecycle.ProfileActive)
			if name != "no session" {
				f.openSession(t, &lifecycle.DemandCounts{Assigned: 2})
			}
			switch name {
			case "partial":
				f.snapshot.Complete = false
			case "stale":
				f.snapshot.ObservedAt = f.now.Add(-2 * time.Minute)
			case "unknown backend limit":
				f.snapshot.BackendLimit = nil
			case "zero backend limit":
				f.snapshot.BackendLimit = new(0)
			}
			result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
			if err != nil || result.ProposedStarts != 0 || len(result.CleanupCandidates) != 0 {
				t.Fatalf("incomplete evidence allowed proposals: %+v, %v", result, err)
			}
			if name == "no session" && (result.Target != nil || result.DemandStatus != "unknown") {
				t.Fatalf("missing demand became zero: %+v", result)
			}
			if name == "zero backend limit" && (!result.CapacityVerified || result.EffectiveMax != 0 || result.Headroom == nil || *result.Headroom != 0) {
				t.Fatalf("verified zero capacity became unavailable data: %+v", result)
			}
			if name == "unknown backend limit" && (result.CapacityVerified || !slices.Contains(result.Blockers, "backend limit unavailable")) {
				t.Fatalf("unknown backend limit became verified capacity: %+v", result)
			}
		})
	}
}

func TestProfileReconciliationClosesProposalsForUnsettledEffectsAndDrainsReserve(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileDraining)
	f.openSession(t, &lifecycle.DemandCounts{Assigned: 2})
	result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || result.EffectiveReserve != 0 || result.ProposedStarts != 2 {
		t.Fatalf("drain failed to serve assigned demand without reserve: %+v, %v", result, err)
	}
	if _, err := f.ledger.PlanEffect(t.Context(), f.lease, "build", lifecycle.Effect{
		ID: "pending", Kind: "provision", Target: "resource", RequestSHA256: strings.Repeat("ab", 32), Deadline: f.now.Add(30 * time.Second),
	}, f.now); err != nil {
		t.Fatal(err)
	}
	result, err = f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || result.ProposedStarts != 0 || !slices.Contains(result.Blockers, "unfinished lifecycle effects") {
		t.Fatalf("unfinished provision allowed overlapping capacity changes: %+v, %v", result, err)
	}
}

func TestProfileReconciliationValidatesLeaseGenerationAndInspectionTime(t *testing.T) {
	for _, name := range []string{"wrong owner", "wrong generation", "wrong digest", "canceled context", "inspection delay", "expiry during inspection"} {
		t.Run(name, func(t *testing.T) {
			f := newReconciliationFixture(t, lifecycle.ProfileActive)
			f.openSession(t, &lifecycle.DemandCounts{Assigned: 1})
			lease := f.lease
			ctx := t.Context()
			switch name {
			case "wrong owner":
				lease.Owner = "other"
			case "wrong generation":
				f.coordinator.Generation++
			case "wrong digest":
				f.coordinator.ConfigurationDigest = strings.Repeat("de", 32)
			case "canceled context":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			case "inspection delay", "expiry during inspection":
				f.coordinator.Provider = inventorySource(func(context.Context, runtimeconfig.Profile) (reconciler.Snapshot, error) {
					f.now = f.now.Add(2 * time.Second)
					if name == "expiry during inspection" {
						f.now = f.now.Add(2 * time.Minute)
					}
					f.snapshot.ObservedAt = f.now
					return f.snapshot, nil
				})
			}
			result, err := f.coordinator.Reconcile(ctx, lease, f.profile)
			if name == "inspection delay" {
				if err != nil || result.ProposedStarts != 2 {
					t.Fatalf("fresh post-inspection snapshot rejected: %+v, %v", result, err)
				}
				return
			}
			if err == nil || result.Target != nil {
				t.Fatalf("invalid ownership/configuration returned a plan: %+v, %v", result, err)
			}
			if (name == "wrong owner" || name == "wrong generation" || name == "wrong digest" || name == "canceled context") && f.inspections != 0 {
				t.Fatal("provider was inspected before ownership/configuration validation")
			}
		})
	}
}

func TestProfileReconciliationRecordsRecoveryEvidenceWithoutCapacityAuthority(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileActive)
	f.openSession(t, &lifecycle.DemandCounts{Assigned: 2})
	binding := f.bind(t, "owned", 0)
	f.now = f.now.Add(2 * time.Minute)
	recovering, err := f.ledger.AcquireLease(t.Context(), f.lease.Deployment, "successor", f.now, time.Minute)
	if err != nil || recovering.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("recovery lease = %+v, %v", recovering, err)
	}
	f.snapshot.ObservedAt = f.now
	f.snapshot.Resources = []reconciler.ObservedResource{f.resource(binding, lifecycle.ResourceRunning)}
	result, err := f.coordinator.Reconcile(t.Context(), recovering, f.profile)
	if err != nil || result.CapacityVerified || result.ProposedStarts != 0 || len(result.CleanupCandidates) != 0 || result.Target != nil ||
		!slices.Contains(result.Blockers, "deployment ownership recovering") {
		t.Fatalf("recovering owner acquired capacity authority or reused old-session demand: %+v, %v", result, err)
	}
	stored := readTestExecutor(t, f.ledger, recovering, "build", binding)
	if stored.Resource.State != lifecycle.ResourceRunning {
		t.Fatalf("recovery failed to persist resource evidence: %+v", stored)
	}
}

func TestProfileReconciliationRejectsUnavailableAndInvalidSnapshots(t *testing.T) {
	for _, name := range []string{"unavailable", "duplicate resource", "future snapshot", "observation after snapshot"} {
		t.Run(name, func(t *testing.T) {
			f := newReconciliationFixture(t, lifecycle.ProfileActive)
			switch name {
			case "unavailable":
				f.coordinator.Provider = inventorySource(func(context.Context, runtimeconfig.Profile) (reconciler.Snapshot, error) {
					return reconciler.Snapshot{}, errors.New("inventory unavailable")
				})
			case "duplicate resource":
				resource := reconciler.ObservedResource{ResourceID: "foreign"}
				f.snapshot.Resources = []reconciler.ObservedResource{resource, resource}
			case "future snapshot":
				f.snapshot.ObservedAt = f.now.Add(time.Second)
			case "observation after snapshot":
				f.snapshot.Resources = []reconciler.ObservedResource{{
					ResourceID:  "foreign",
					Observation: lifecycle.ResourceObservation{ObservedAt: f.now.Add(time.Second)},
				}}
			}
			result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
			if err == nil || result.CapacityVerified || result.Target != nil {
				t.Fatalf("invalid/unavailable inventory became usable capacity: %+v, %v", result, err)
			}
		})
	}
}

func TestUnverifiedIncarnationDoesNotBecomeAConfirmedIdentityConflict(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileActive)
	f.openSession(t, &lifecycle.DemandCounts{})
	binding := f.bind(t, "owned", 0)
	resource := f.resource(binding, lifecycle.ResourceRunning)
	resource.Incarnation = ""
	f.snapshot.Resources = []reconciler.ObservedResource{resource}
	result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || result.Unknown == nil || *result.Unknown != 1 || len(result.CleanupCandidates) != 0 {
		t.Fatalf("unverified incarnation became usable: %+v, %v", result, err)
	}
	stored := readTestExecutor(t, f.ledger, f.lease, "build", binding)
	if stored.Conflicted {
		t.Fatal("missing identity proof was persisted as a contradictory identity")
	}
	f.snapshot.Resources[0].Incarnation = binding.Incarnation
	result, err = f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || len(result.OrphanCandidates) != 0 {
		t.Fatalf("matching identity evidence failed to resolve uncertainty: %+v, %v", result, err)
	}
	stored = readTestExecutor(t, f.ledger, f.lease, "build", binding)
	if stored.Conflicted || stored.Resource.State != lifecycle.ResourceRunning {
		t.Fatalf("matching resource evidence was not retained: %+v", stored)
	}
}

func TestProfileReconciliationFollowsAllocationQueryPages(t *testing.T) {
	f := newReconciliationFixture(t, lifecycle.ProfileActive)
	f.openSession(t, &lifecycle.DemandCounts{Assigned: 2})
	first := f.bind(t, "a", 0)
	second := f.bind(t, "b", 0)
	f.snapshot.BackendLimit = new(2)
	f.snapshot.Resources = []reconciler.ObservedResource{
		f.resource(first, lifecycle.ResourceStarting), f.resource(second, lifecycle.ResourceStarting),
	}
	firstKey := lifecycle.ProfileKey(f.lease.Deployment, f.profile.ID, "RUNNER#a")
	secondKey := lifecycle.ProfileKey(f.lease.Deployment, f.profile.ID, "RUNNER#b")
	defaultQuery := f.db.query
	var pages int
	f.db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if stringValue(input.ExpressionAttributeValues[":prefix"]) != "RUNNER#" {
			return defaultQuery(input)
		}
		pages++
		if len(input.ExclusiveStartKey) == 0 {
			return &dynamodb.QueryOutput{Items: []attributes{f.db.items[firstKey]}, LastEvaluatedKey: keyAttributes(firstKey)}, nil
		}
		if keyOf(input.ExclusiveStartKey) != firstKey {
			return nil, errors.New("allocation query cursor did not advance correctly")
		}
		return &dynamodb.QueryOutput{Items: []attributes{f.db.items[secondKey]}}, nil
	}
	result, err := f.coordinator.Reconcile(t.Context(), f.lease, f.profile)
	if err != nil || pages != 2 || result.CrediblePending == nil || *result.CrediblePending != 2 ||
		result.Occupied == nil || *result.Occupied != 2 || result.ProposedStarts != 0 {
		t.Fatalf("allocation pagination lost capacity: %+v, pages = %d, error = %v", result, pages, err)
	}
}
