package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestExecutorBindingIsFencedAndExclusive(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	const profile = "build"
	first := lifecycle.ExecutorBinding{AllocationID: "first", ResourceID: "resource-1", Incarnation: "boot-1"}
	second := lifecycle.ExecutorBinding{AllocationID: "second", ResourceID: "resource-1", Incarnation: "boot-1"}
	runner := lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"}

	if err := ledger.BindExecutor(ctx, lease, profile, first, now.Add(time.Minute)); !errors.Is(err, lifecycle.ErrLeaseLost) {
		t.Fatalf("expired lease binding: %v", err)
	}
	bindTestExecutor(t, ledger, lease, profile, first, runner)
	bindTestExecutor(t, ledger, lease, profile, first, runner)
	if err := ledger.BindExecutor(ctx, lease, profile, second, now); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("duplicate resource binding: %v", err)
	}
	if got, err := ledger.Executor(ctx, lease.Deployment, profile, second.AllocationID); err != nil || got != nil {
		t.Fatalf("rejected resource claim created an allocation: %+v, %v", got, err)
	}
	other, err := ledger.AcquireLease(ctx, "another-deployment", "controller", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.BindExecutor(ctx, other, profile, first, now); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("cross-deployment resource claim: %v", err)
	}
	second.ResourceID = "resource-2"
	if err := ledger.BindExecutor(ctx, lease, profile, second, now); err != nil {
		t.Fatal(err)
	}
	wrongBoot := second
	wrongBoot.Incarnation = "boot-2"
	if err := ledger.BindExecutorRunner(ctx, lease, profile, wrongBoot, runner, now); !errors.Is(err, lifecycle.ErrExecutorIdentity) {
		t.Fatalf("wrong incarnation registration: %v", err)
	}
	if err := ledger.BindExecutorRunner(ctx, lease, profile, second, runner, now); !errors.Is(err, lifecycle.ErrConflict) {
		t.Fatalf("duplicate runner registration: %v", err)
	}
	if got := readTestExecutor(t, ledger, lease, profile, second); got.Runner != (lifecycle.RunnerIdentity{}) {
		t.Fatalf("rejected runner claim changed the allocation: %+v", got.Runner)
	}
	if got := readTestExecutor(t, ledger, lease, profile, first); got.Binding != first || got.Runner != runner {
		t.Fatalf("original binding changed: %+v", got)
	}
}

func TestDeliveryProjectsExactRunnerEvidence(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	first := lifecycle.ExecutorBinding{AllocationID: "first", ResourceID: "resource-1", Incarnation: "boot-1"}
	second := lifecycle.ExecutorBinding{AllocationID: "second", ResourceID: "resource-2", Incarnation: "boot-2"}
	bindTestExecutor(t, ledger, lease, profile, first, lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"})
	bindTestExecutor(t, ledger, lease, profile, second, lifecycle.RunnerIdentity{ID: 8, Name: "runner-8"})
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
		t.Fatal(err)
	}
	completed := lifecycle.JobFact{RequestID: 100, RunnerID: 7, Phase: lifecycle.JobCompleted, OccurredAt: now}
	started := lifecycle.JobFact{RequestID: 101, RunnerID: 8, Phase: lifecycle.JobStarted}
	delivery := lifecycle.Delivery{MessageID: 1, Jobs: []lifecycle.JobFact{
		completed,
		{RequestID: 100, RunnerID: 7, Phase: lifecycle.JobStarted, OccurredAt: now.Add(-time.Second)},
		started,
		{RequestID: 101, Phase: lifecycle.JobCompleted},
		{RequestID: 102, RunnerID: 9, Phase: lifecycle.JobCompleted},
	}}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
		t.Fatal(err)
	}
	if got := readTestExecutor(t, ledger, lease, profile, first); got.Conflicted || got.Attempt != completed {
		t.Fatalf("reordered start replaced terminal evidence: %+v", got)
	}
	if got := readTestExecutor(t, ledger, lease, profile, second); got.Conflicted || got.Attempt != started {
		t.Fatalf("identity-less completion changed runner evidence: %+v", got)
	}
}

func TestDeliveryAtomicallyPreservesConflictingExecutorEvidenceOnReplay(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, db, lease := admissionStore(t, now)
	const profile, session = "build", "owned"
	first := lifecycle.ExecutorBinding{AllocationID: "first", ResourceID: "resource-1", Incarnation: "boot-1"}
	second := lifecycle.ExecutorBinding{AllocationID: "second", ResourceID: "resource-2", Incarnation: "boot-2"}
	bindTestExecutor(t, ledger, lease, profile, first, lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"})
	bindTestExecutor(t, ledger, lease, profile, second, lifecycle.RunnerIdentity{ID: 8, Name: "runner-8"})
	if err := ledger.BeginDeliverySession(ctx, lease, profile, "", session, nil, now); err != nil {
		t.Fatal(err)
	}
	firstFact := lifecycle.JobFact{RequestID: 100, RunnerID: 7, Phase: lifecycle.JobStarted}
	delivery := lifecycle.Delivery{MessageID: 1, Jobs: []lifecycle.JobFact{
		firstFact, {RequestID: 100, RunnerID: 8, Phase: lifecycle.JobCompleted},
	}}
	interrupted := errors.New("interrupted executor observation")
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, item := range input.TransactItems {
			if item.Put != nil && keyOf(item.Put.Item).SK == "RUNNER#second" {
				return interrupted
			}
		}
		return nil
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted delivery: %v", err)
	}
	if got := readTestExecutor(t, ledger, lease, profile, first); got.Conflicted || got.Attempt != firstFact {
		t.Fatalf("failed transaction altered first executor: %+v", got)
	}
	db.beforeWrite = nil
	ledger, err := lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.PersistDelivery(ctx, lease, profile, session, now, delivery); err != nil {
		t.Fatal(err)
	}
	if got := readTestExecutor(t, ledger, lease, profile, first); !got.Conflicted || got.Attempt != firstFact {
		t.Fatalf("replay lost the first runner's evidence: %+v", got)
	}
	if got := readTestExecutor(t, ledger, lease, profile, second); !got.Conflicted || got.Attempt != (lifecycle.JobFact{}) {
		t.Fatalf("replay assigned a contradictory completion: %+v", got)
	}
}

func bindTestExecutor(t *testing.T, ledger *lifecycle.Store, lease lifecycle.Lease, profile string, binding lifecycle.ExecutorBinding, runner lifecycle.RunnerIdentity) {
	t.Helper()
	if err := ledger.BindExecutor(t.Context(), lease, profile, binding, admissionTime); err != nil {
		t.Fatal(err)
	}
	if err := ledger.BindExecutorRunner(t.Context(), lease, profile, binding, runner, admissionTime); err != nil {
		t.Fatal(err)
	}
}

func readTestExecutor(t *testing.T, ledger *lifecycle.Store, lease lifecycle.Lease, profile string, binding lifecycle.ExecutorBinding) lifecycle.Executor {
	t.Helper()
	got, err := ledger.Executor(t.Context(), lease.Deployment, profile, binding.AllocationID)
	if err != nil || got == nil {
		t.Fatalf("read executor %q: %+v, %v", binding.AllocationID, got, err)
	}
	return *got
}
