package reconciler_test

import (
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/planner"
	"github.com/knightfall/scaleset-example-aws/reconciler"
)

var observedAt = time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

func TestExecutorClassificationProtectsActiveAndIncompleteEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, resource, process, job, want string
		conflicted                         bool
	}{
		{"legacy record", "", "", "", "unknown", false},
		{"running resource without bootstrap", lifecycle.ResourceRunning, "", "", "unknown", false},
		{"booting resource", lifecycle.ResourceStarting, "", "", "starting", false},
		{"ready runner", lifecycle.ResourceRunning, lifecycle.ProcessReady, "", "idle", false},
		{"active job", lifecycle.ResourceRunning, lifecycle.ProcessReady, lifecycle.JobStarted, "busy", false},
		{"start precedes readiness", lifecycle.ResourceRunning, "", lifecycle.JobStarted, "busy", false},
		{"process exited during job", lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobStarted, "unknown", false},
		{"completion without exit", lifecycle.ResourceRunning, lifecycle.ProcessReady, lifecycle.JobCompleted, "unknown", false},
		{"matching completion and exit", lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobCompleted, "completed", false},
		{"exit without known job", lifecycle.ResourceRunning, lifecycle.ProcessExited, "", "unknown", false},
		{"terminal provider", lifecycle.ResourceTerminal, "", "", "lost", false},
		{"missing provider", lifecycle.ResourceUnknown, lifecycle.ProcessExited, lifecycle.JobCompleted, "unknown", false},
		{"conflicting completion", lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobCompleted, "unknown", true},
		{"job on starting resource", lifecycle.ResourceStarting, "", lifecycle.JobStarted, "unknown", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := executorEvidence(tc.resource, tc.process, tc.job)
			e.Conflicted = tc.conflicted
			result, err := reconciler.ClassifyExecutor(e, observedAt, time.Minute, 5*time.Second)
			if err != nil || result.State != tc.want || !result.Owned || result.ID != e.Binding.ResourceID {
				t.Fatalf("classification = %+v, want %q, error = %v", result, tc.want, err)
			}
		})
	}
}

func executorEvidence(resource, process, job string) lifecycle.Executor {
	runner := lifecycle.RunnerIdentity{ID: 7, Name: "runner-7"}
	e := lifecycle.Executor{
		Binding: lifecycle.ExecutorBinding{AllocationID: "owned", ResourceID: "resource-1", Incarnation: "boot-1"},
		Runner:  runner,
	}
	if resource != "" {
		e.Resource = lifecycle.ResourceObservation{
			State: resource, ObservedAt: observedAt, StartedAt: observedAt.Add(-30 * time.Second), Reference: "inventory:1",
		}
	}
	if process != "" {
		e.Process = lifecycle.ProcessObservation{Runner: runner, State: process, ObservedAt: observedAt.Add(-10 * time.Second), Reference: "bootstrap:1"}
	}
	if job != "" {
		e.Attempt = lifecycle.JobFact{RequestID: 100, RunnerID: runner.ID, Phase: job, OccurredAt: observedAt.Add(-20 * time.Second)}
	}
	return e
}

func TestExecutorClassificationRequiresFreshResourceAndExactProcessEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*lifecycle.Executor)
	}{
		{"stale resource", func(e *lifecycle.Executor) { e.Resource.ObservedAt = observedAt.Add(-time.Minute - time.Nanosecond) }},
		{"future resource", func(e *lifecycle.Executor) { e.Resource.ObservedAt = observedAt.Add(time.Nanosecond) }},
		{"missing resource proof", func(e *lifecycle.Executor) { e.Resource.Reference = "" }},
		{"another process", func(e *lifecycle.Executor) { e.Process.Runner.ID++ }},
		{"missing process proof", func(e *lifecycle.Executor) { e.Process.Reference = "" }},
		{"future exit", func(e *lifecycle.Executor) { e.Process.ObservedAt = observedAt.Add(time.Nanosecond) }},
		{"exit grace incomplete", func(e *lifecycle.Executor) { e.Process.ObservedAt = observedAt.Add(-5*time.Second + time.Nanosecond) }},
		{"another job runner", func(e *lifecycle.Executor) { e.Attempt.RunnerID++ }},
		{"ready evidence after exit", func(e *lifecycle.Executor) { e.ProcessLiveAt = e.Process.ObservedAt.Add(time.Nanosecond) }},
		{"future live evidence", func(e *lifecycle.Executor) { e.ResourceLiveAt = observedAt.Add(time.Nanosecond) }},
		{"live evidence at provider termination", func(e *lifecycle.Executor) {
			e.Resource.State = lifecycle.ResourceTerminal
			e.ResourceLiveAt = e.Resource.ObservedAt
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := executorEvidence(lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobCompleted)
			tc.modify(&e)
			result, err := reconciler.ClassifyExecutor(e, observedAt, time.Minute, 5*time.Second)
			if err != nil || result.State != "unknown" {
				t.Fatalf("unsafe evidence classified as %+v, error = %v", result, err)
			}
		})
	}
}

func TestExecutorClassificationComposesWithCapacityAndDrainPlanning(t *testing.T) {
	busy := executorEvidence(lifecycle.ResourceRunning, lifecycle.ProcessReady, lifecycle.JobStarted)
	completed := executorEvidence(lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobCompleted)
	completed.Binding.ResourceID = "resource-2"
	inventory := planner.Inventory{ObservedAt: observedAt, Complete: true}
	for _, executor := range []lifecycle.Executor{busy, completed} {
		entry, err := reconciler.ClassifyExecutor(executor, observedAt, time.Minute, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		inventory.Executors = append(inventory.Executors, entry)
	}
	limits := planner.Limits{MaxRunners: 2, IdleReserve: 2, Draining: true, StartupTimeout: time.Minute}
	result, err := planner.Calculate(limits, &planner.Demand{ObservedAt: observedAt, Assigned: 1, Running: 1}, &inventory, observedAt, time.Minute)
	if err != nil || result.EffectiveReserve != 0 || result.ProposedStarts != 0 ||
		len(result.CleanupCandidates) != 1 || result.CleanupCandidates[0] != completed.Binding.ResourceID {
		t.Fatalf("drain affected an active executor or retained reserve: %+v, %v", result, err)
	}
	if result.Occupied == nil || *result.Occupied != 2 || result.Busy == nil || *result.Busy != 1 {
		t.Fatalf("capacity ignored protected occupancy: %+v", result)
	}
}

func TestExecutorClassificationHonorsFreshnessAndGraceBoundaries(t *testing.T) {
	e := executorEvidence(lifecycle.ResourceRunning, lifecycle.ProcessExited, lifecycle.JobCompleted)
	e.Resource.ObservedAt = observedAt.Add(-time.Minute)
	e.Resource.StartedAt = e.Resource.ObservedAt.Add(-time.Minute)
	e.Process.ObservedAt = observedAt.Add(-5 * time.Second)
	result, err := reconciler.ClassifyExecutor(e, observedAt, time.Minute, 5*time.Second)
	if err != nil || result.State != "completed" {
		t.Fatalf("exact freshness/grace boundary rejected: %+v, %v", result, err)
	}
	if _, err := reconciler.ClassifyExecutor(e, observedAt, time.Minute, 0); err == nil {
		t.Fatal("accepted an absent cleanup grace window")
	}
	for _, finish := range []time.Time{{}, observedAt.Add(time.Minute)} {
		e.Attempt.OccurredAt = finish
		result, err = reconciler.ClassifyExecutor(e, observedAt, time.Minute, 5*time.Second)
		if err != nil || result.State != "completed" {
			t.Fatalf("remote finish-time skew invalidated exact terminal evidence: %+v, %v", result, err)
		}
	}
	ready := executorEvidence(lifecycle.ResourceRunning, lifecycle.ProcessReady, "")
	ready.Process.ObservedAt = observedAt.Add(-time.Minute - time.Nanosecond)
	result, err = reconciler.ClassifyExecutor(ready, observedAt, time.Minute, time.Second)
	if err != nil || result.State != "unknown" {
		t.Fatalf("stale readiness counted as serving: %+v, %v", result, err)
	}
}
