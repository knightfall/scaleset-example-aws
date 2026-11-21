// Package reconciler translates durable lifecycle evidence into capacity decisions.
package reconciler

import (
	"errors"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/planner"
)

// ClassifyExecutor protects incomplete or contradictory evidence as unknown.
// Its output is a planner input. Cleanup still requires a fenced effect and a
// fresh identity check at dispatch.
func ClassifyExecutor(executor lifecycle.Executor, now time.Time, maxAge, exitGrace time.Duration) (planner.Executor, error) {
	result := planner.Executor{ID: executor.Binding.ResourceID, Owned: true, State: "unknown"}
	if result.ID == "" || executor.Binding.AllocationID == "" || executor.Binding.Incarnation == "" ||
		now.IsZero() || maxAge <= 0 || exitGrace <= 0 {
		return result, errors.New("executor classification requires identity and valid observation timing")
	}
	if executor.Conflicted || executor.ConflictReference != "" || executor.ResourceLiveAt.After(now) || executor.ProcessLiveAt.After(now) {
		return result, nil
	}
	resource := executor.Resource
	if resource.ObservedAt.IsZero() || resource.ObservedAt.After(now) || resource.Reference == "" {
		return result, nil
	}
	if resource.State == lifecycle.ResourceTerminal {
		if !executor.ResourceLiveAt.IsZero() && !executor.ResourceLiveAt.Before(resource.ObservedAt) {
			return result, nil
		}
		result.State = "lost"
		return result, nil
	}
	if now.Sub(resource.ObservedAt) > maxAge {
		return result, nil
	}
	if resource.State == lifecycle.ResourceStarting {
		processUnknown := executor.Process.State == "" || executor.Process.State == lifecycle.ProcessUnknown
		if !resource.StartedAt.IsZero() && !resource.StartedAt.After(resource.ObservedAt) && executor.Attempt.RequestID == 0 && processUnknown {
			result.State, result.StartedAt = "starting", resource.StartedAt
		}
		return result, nil
	}
	if resource.State != lifecycle.ResourceRunning {
		return result, nil
	}
	if executor.Runner.ID <= 0 || executor.Runner.Name == "" {
		return result, nil
	}
	job := executor.Attempt
	if job.RequestID > 0 && job.RunnerID == executor.Runner.ID && job.Phase == lifecycle.JobStarted {
		if executor.Process.State != lifecycle.ProcessExited {
			result.State = "busy"
		}
		return result, nil
	}
	process := executor.Process
	if process.Runner != executor.Runner || process.Reference == "" || process.ObservedAt.IsZero() || process.ObservedAt.After(now) {
		return result, nil
	}
	if job.RequestID > 0 && job.RunnerID == executor.Runner.ID && job.Phase == lifecycle.JobCompleted {
		if !executor.ProcessLiveAt.IsZero() && !executor.ProcessLiveAt.Before(process.ObservedAt) {
			return result, nil
		}
		// Exact runner identity links these terminal facts. Their timestamps come
		// from different clocks, so grace uses the local process observation.
		if process.State == lifecycle.ProcessExited && now.Sub(process.ObservedAt) >= exitGrace {
			result.State = "completed"
		}
		return result, nil
	}
	if job == (lifecycle.JobFact{}) && process.State == lifecycle.ProcessReady && now.Sub(process.ObservedAt) <= maxAge {
		result.State = "idle"
	}
	return result, nil
}
