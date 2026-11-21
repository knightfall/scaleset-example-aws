package lifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestExecutorObservationsPersistExactIdentityAndProtectTerminalEvidence(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, db, lease := admissionStore(t, now)
	binding := lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"}
	runner := lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"}
	bindTestExecutor(t, ledger, lease, "build", binding, runner)
	resource := lifecycle.ResourceObservation{
		State: lifecycle.ResourceRunning, ObservedAt: now, StartedAt: now.Add(-time.Second), Reference: "inventory:1",
	}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, resource, now); err != nil {
		t.Fatal(err)
	}
	offset := resource
	offset.ObservedAt = offset.ObservedAt.In(time.FixedZone("fixture", 3600))
	if err := ledger.ObserveResource(ctx, lease, "build", binding, offset, now); err != nil {
		t.Fatal(err)
	}
	exit := lifecycle.ProcessObservation{Runner: runner, State: lifecycle.ProcessExited, ObservedAt: now, Reference: "bootstrap:exit"}
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, exit, now); err != nil {
		t.Fatal(err)
	}
	// A delayed ready report must not resurrect the one-job process.
	ready := exit
	ready.State, ready.ObservedAt = lifecycle.ProcessReady, now.Add(-time.Second)
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, ready, now); err != nil {
		t.Fatal(err)
	}
	// Reobserving exit retains the first proof time used for log-flush grace.
	replayed := exit
	replayed.ObservedAt = now.Add(time.Second)
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, replayed, replayed.ObservedAt); err != nil {
		t.Fatal(err)
	}
	restarted, err := lifecycle.NewStore(db, "state")
	if err != nil {
		t.Fatal(err)
	}
	got := readTestExecutor(t, restarted, lease, "build", binding)
	if got.Resource != resource || got.Process != exit || got.Conflicted {
		t.Fatalf("restart lost monotonic evidence: %+v", got)
	}
	ready.ObservedAt = now.Add(2 * time.Second)
	if err := restarted.ObserveProcess(ctx, lease, "build", binding, ready, ready.ObservedAt); err != nil {
		t.Fatal(err)
	}
	got = readTestExecutor(t, restarted, lease, "build", binding)
	if !got.Conflicted || got.Process != exit {
		t.Fatalf("new readiness after exit failed to protect the executor: %+v", got)
	}
}

func TestExecutorObservationsRejectWrongIdentityInvalidTimeAndExpiredLease(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	binding := lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"}
	runner := lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"}
	bindTestExecutor(t, ledger, lease, "build", binding, runner)
	resource := lifecycle.ResourceObservation{State: lifecycle.ResourceRunning, ObservedAt: now, Reference: "inventory:1"}
	wrong := binding
	wrong.Incarnation = "boot-2"
	if err := ledger.ObserveResource(ctx, lease, "build", wrong, resource, now); !errors.Is(err, lifecycle.ErrExecutorIdentity) {
		t.Fatalf("wrong resource incarnation accepted: %v", err)
	}
	process := lifecycle.ProcessObservation{
		Runner: lifecycle.RunnerIdentity{ID: 8, Name: "other"}, State: lifecycle.ProcessReady,
		ObservedAt: now, Reference: "bootstrap:ready",
	}
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, process, now); !errors.Is(err, lifecycle.ErrExecutorIdentity) {
		t.Fatalf("wrong process identity accepted: %v", err)
	}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, resource, now.Add(time.Minute)); !errors.Is(err, lifecycle.ErrLeaseLost) {
		t.Fatalf("expired observer could write: %v", err)
	}
	resource.ObservedAt = now.Add(time.Second)
	if err := ledger.ObserveResource(ctx, lease, "build", binding, resource, now); err == nil {
		t.Fatal("future resource observation accepted")
	}
	got := readTestExecutor(t, ledger, lease, "build", binding)
	if got.Resource != (lifecycle.ResourceObservation{}) || got.Process != (lifecycle.ProcessObservation{}) || got.Conflicted {
		t.Fatalf("rejected observations changed durable state: %+v", got)
	}
}

func TestResourceObservationRetainsStartupDeadlineAndDetectsResurrection(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	binding := lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"}
	if err := ledger.BindExecutor(ctx, lease, "build", binding, now); err != nil {
		t.Fatal(err)
	}
	starting := lifecycle.ResourceObservation{
		State: lifecycle.ResourceStarting, ObservedAt: now, StartedAt: now.Add(-time.Second), Reference: "inventory:1",
	}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, starting, now); err != nil {
		t.Fatal(err)
	}
	running := lifecycle.ResourceObservation{State: lifecycle.ResourceRunning, ObservedAt: now.Add(time.Second), Reference: "inventory:2"}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, running, running.ObservedAt); err != nil {
		t.Fatal(err)
	}
	got := readTestExecutor(t, ledger, lease, "build", binding)
	if got.Resource.StartedAt != starting.StartedAt {
		t.Fatal("inventory refresh moved the startup deadline")
	}
	terminal := lifecycle.ResourceObservation{State: lifecycle.ResourceTerminal, ObservedAt: now.Add(2 * time.Second), Reference: "inventory:terminal"}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, terminal, terminal.ObservedAt); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, running, terminal.ObservedAt); err != nil {
		t.Fatal(err)
	}
	got = readTestExecutor(t, ledger, lease, "build", binding)
	if got.Conflicted || got.Resource.State != lifecycle.ResourceTerminal {
		t.Fatalf("delayed inventory resurrected terminal resource: %+v", got)
	}
	running.ObservedAt = now.Add(3 * time.Second)
	if err := ledger.ObserveResource(ctx, lease, "build", binding, running, running.ObservedAt); err != nil {
		t.Fatal(err)
	}
	if got := readTestExecutor(t, ledger, lease, "build", binding); !got.Conflicted || got.Resource.State != lifecycle.ResourceTerminal {
		t.Fatalf("new running evidence after terminal was not protected: %+v", got)
	}
}

func TestRecoveringOwnerCanRecordEvidenceWithoutAdoptingCapacity(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	binding := lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"}
	if err := ledger.BindExecutor(ctx, lease, "build", binding, now); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	recovering, err := ledger.AcquireLease(ctx, "dev", "successor", now, time.Minute)
	if err != nil || recovering.Phase != lifecycle.LeaseRecovering {
		t.Fatalf("successor lease = %+v, %v", recovering, err)
	}
	resource := lifecycle.ResourceObservation{State: lifecycle.ResourceTerminal, ObservedAt: now, Reference: "inventory:terminated"}
	if err := ledger.ObserveResource(ctx, recovering, "build", binding, resource, now); err != nil {
		t.Fatal(err)
	}
	if got := readTestExecutor(t, ledger, recovering, "build", binding); got.Resource != resource {
		t.Fatalf("recovering owner lost provider evidence: %+v", got)
	}
	binding.AllocationID, binding.ResourceID = "new", "resource-2"
	if err := ledger.BindExecutor(ctx, recovering, "build", binding, now); !errors.Is(err, lifecycle.ErrRecoveryRequired) {
		t.Fatalf("recovering owner adopted new capacity: %v", err)
	}
}

func TestDelayedTerminalObservationProtectsAgainstLaterLiveEvidence(t *testing.T) {
	ctx := t.Context()
	now := admissionTime
	ledger, _, lease := admissionStore(t, now)
	binding := lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"}
	runner := lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"}
	bindTestExecutor(t, ledger, lease, "build", binding, runner)
	running := lifecycle.ResourceObservation{State: lifecycle.ResourceRunning, ObservedAt: now, Reference: "inventory:live"}
	ready := lifecycle.ProcessObservation{Runner: runner, State: lifecycle.ProcessReady, ObservedAt: now, Reference: "bootstrap:ready"}
	if err := ledger.ObserveResource(ctx, lease, "build", binding, running, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, ready, now); err != nil {
		t.Fatal(err)
	}
	exit := ready
	exit.State, exit.ObservedAt, exit.Reference = lifecycle.ProcessExited, now.Add(-time.Second), "bootstrap:exit"
	if err := ledger.ObserveProcess(ctx, lease, "build", binding, exit, now); err != nil {
		t.Fatal(err)
	}
	got := readTestExecutor(t, ledger, lease, "build", binding)
	if !got.Conflicted || got.Process != exit {
		t.Fatalf("delayed exit disappeared behind readiness: %+v", got)
	}
	terminal := running
	terminal.State, terminal.ObservedAt, terminal.Reference = lifecycle.ResourceTerminal, now.Add(-time.Second), "inventory:terminal"
	if err := ledger.ObserveResource(ctx, lease, "build", binding, terminal, now); err != nil {
		t.Fatal(err)
	}
	got = readTestExecutor(t, ledger, lease, "build", binding)
	if !got.Conflicted || got.Resource != terminal {
		t.Fatalf("delayed termination disappeared behind presence: %+v", got)
	}
}
