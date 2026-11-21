package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

var ErrExecutorIdentity = errors.New("executor identity does not match its allocation")

// ExecutorBinding identifies an owned provider resource and its exact incarnation.
// The caller verifies provider ownership before binding the allocation.
type ExecutorBinding struct {
	AllocationID string `json:"allocationId"`
	ResourceID   string `json:"resourceId"`
	Incarnation  string `json:"incarnation"`
}

type RunnerIdentity struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Executor stores verified identities and job observations. A completed job
// alone provides no evidence that the executor can be stopped.
type Executor struct {
	Binding        ExecutorBinding     `json:"binding"`
	Runner         RunnerIdentity      `json:"runner,omitzero"`
	Attempt        JobFact             `json:"attempt,omitzero"`
	Resource       ResourceObservation `json:"resource,omitzero"`
	Process        ProcessObservation  `json:"process,omitzero"`
	ResourceLiveAt time.Time           `json:"resourceLiveAt,omitzero"`
	ProcessLiveAt  time.Time           `json:"processLiveAt,omitzero"`
	Conflicted     bool                `json:"conflicted,omitempty"`
	revision       int64
}

func (b ExecutorBinding) valid() bool {
	return b.AllocationID != "" && !strings.Contains(b.AllocationID, "#") &&
		strings.TrimSpace(b.ResourceID) != "" && strings.TrimSpace(b.Incarnation) != ""
}

func (r RunnerIdentity) valid() bool {
	return r.ID > 0 && strings.TrimSpace(r.Name) != ""
}

func executorKey(deployment, profile, allocation string) Key {
	return ProfileKey(deployment, profile, "RUNNER#"+allocation)
}

func executorRunnerKey(deployment, profile string, runnerID int) Key {
	return ProfileKey(deployment, profile, "RUNNER-ID#"+strconv.Itoa(runnerID))
}

func (s *Store) Executor(ctx context.Context, deployment, profile, allocation string) (*Executor, error) {
	record, err := s.Get(ctx, executorKey(deployment, profile, allocation))
	if err != nil || record == nil {
		return nil, err
	}
	var executor Executor
	if err := json.Unmarshal(record.Payload, &executor); err != nil || !executor.valid(allocation) {
		return nil, errors.New("invalid durable executor")
	}
	executor.revision = record.Revision
	return &executor, nil
}

func (e Executor) valid(allocation string) bool {
	if e.Binding.AllocationID != allocation || !e.Binding.valid() ||
		(e.Runner != (RunnerIdentity{}) && !e.Runner.valid()) ||
		(e.Resource != (ResourceObservation{}) && !e.Resource.valid()) ||
		(e.Process != (ProcessObservation{}) && (!e.Process.valid() || e.Process.Runner != e.Runner)) {
		return false
	}
	return e.Attempt == (JobFact{}) || e.Attempt.RequestID > 0 && e.Runner.ID != 0 &&
		e.Attempt.RunnerID == e.Runner.ID && (e.Attempt.Phase == JobStarted || e.Attempt.Phase == JobCompleted)
}

// BindExecutor claims the immutable provider resource and records the allocation
// together. Replaying the same binding leaves recorded job evidence intact.
func (s *Store) BindExecutor(ctx context.Context, lease Lease, profile string, binding ExecutorBinding, now time.Time) error {
	if !binding.valid() {
		return ErrExecutorIdentity
	}
	current, err := s.Executor(ctx, lease.Deployment, profile, binding.AllocationID)
	if err != nil {
		return err
	}
	if current != nil {
		if current.Binding != binding {
			return ErrExecutorIdentity
		}
		return nil
	}
	claim, err := s.profileClaim(ctx, lease.Deployment, profile+"#"+binding.AllocationID, "executor:"+binding.ResourceID)
	if err != nil {
		return err
	}
	executor := Executor{Binding: binding}
	change, err := executor.change(lease.Deployment, profile)
	if err != nil {
		return err
	}
	return s.ApplyFenced(ctx, lease, now, "", claim, change)
}

// BindExecutorRunner links a verified GitHub registration to its allocation.
// Persist the association before releasing a JIT configuration to the runner.
func (s *Store) BindExecutorRunner(
	ctx context.Context, lease Lease, profile string, binding ExecutorBinding, runner RunnerIdentity, now time.Time,
) error {
	if !binding.valid() || !runner.valid() {
		return ErrExecutorIdentity
	}
	executor, err := s.Executor(ctx, lease.Deployment, profile, binding.AllocationID)
	if err != nil {
		return err
	}
	if executor == nil || executor.Binding != binding {
		return ErrExecutorIdentity
	}
	if executor.Runner == runner {
		return nil
	}
	if executor.Runner != (RunnerIdentity{}) {
		return ErrExecutorIdentity
	}
	executor.Runner = runner
	mapping, err := admissionChange(lease.Deployment, executorRunnerKey(lease.Deployment, profile, runner.ID), 0, binding.AllocationID)
	if err != nil {
		return err
	}
	change, err := executor.change(lease.Deployment, profile)
	if err != nil {
		return err
	}
	return s.ApplyFenced(ctx, lease, now, "", mapping, change)
}

func (e Executor) change(deployment, profile string) (Change, error) {
	return admissionChange(deployment, executorKey(deployment, profile, e.Binding.AllocationID), e.revision, e)
}

func (s *Store) executorForRunner(ctx context.Context, deployment, profile string, runnerID int) (*Executor, error) {
	record, err := s.Get(ctx, executorRunnerKey(deployment, profile, runnerID))
	if err != nil || record == nil {
		return nil, err
	}
	var allocation string
	if err := json.Unmarshal(record.Payload, &allocation); err != nil || allocation == "" || strings.Contains(allocation, "#") {
		return nil, errors.New("invalid executor runner lookup")
	}
	executor, err := s.Executor(ctx, deployment, profile, allocation)
	if err != nil {
		return nil, err
	}
	if executor == nil || executor.Runner.ID != runnerID {
		return nil, ErrExecutorIdentity
	}
	return executor, nil
}
