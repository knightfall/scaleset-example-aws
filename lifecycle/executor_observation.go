package lifecycle

import (
	"context"
	"errors"
	"time"
)

const (
	ResourceUnknown  = "unknown"
	ResourceStarting = "starting"
	ResourceRunning  = "running"
	ResourceTerminal = "terminal"
	ProcessUnknown   = "unknown"
	ProcessReady     = "ready"
	ProcessExited    = "exited"
)

// ResourceTerminal means the exact incarnation is irreversibly gone. An
// omitted inventory entry or a stopped, restartable EC2 instance is unknown.
type ResourceObservation struct {
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observedAt"`
	StartedAt  time.Time `json:"startedAt,omitzero"`
	Reference  string    `json:"reference"`
}

// Process observations come from the authenticated, one-job bootstrap protocol.
// A GitHub runner reference alone cannot establish process readiness or exit.
type ProcessObservation struct {
	Runner     RunnerIdentity `json:"runner"`
	State      string         `json:"state"`
	ObservedAt time.Time      `json:"observedAt"`
	Reference  string         `json:"reference"`
}

func (o ResourceObservation) valid() bool {
	if o.Reference == "" || o.ObservedAt.IsZero() || o.StartedAt.After(o.ObservedAt) {
		return false
	}
	switch o.State {
	case ResourceUnknown, ResourceRunning, ResourceTerminal:
		return true
	case ResourceStarting:
		return !o.StartedAt.IsZero()
	default:
		return false
	}
}

func (o ProcessObservation) valid() bool {
	if !o.Runner.valid() || o.Reference == "" || o.ObservedAt.IsZero() {
		return false
	}
	return o.State == ProcessUnknown || o.State == ProcessReady || o.State == ProcessExited
}

func (s *Store) ObserveResource(ctx context.Context, lease Lease, profile string, binding ExecutorBinding, observation ResourceObservation, now time.Time) error {
	if !observation.valid() || now.IsZero() || observation.ObservedAt.After(now) {
		return errors.New("invalid resource observation")
	}
	observation.ObservedAt = observation.ObservedAt.UTC()
	observation.StartedAt = observation.StartedAt.UTC()
	executor, err := s.boundExecutor(ctx, lease.Deployment, profile, binding)
	if err != nil {
		return err
	}
	prior := executor.Resource
	lateTerminal := observation.State == ResourceTerminal && prior.State != ResourceTerminal
	older := observation.ObservedAt.Before(prior.ObservedAt)
	if older && !lateTerminal {
		return nil
	}
	if older && prior.State != ResourceUnknown {
		executor.Conflicted = true
	}
	if prior.State == ResourceTerminal {
		if observation.State == ResourceTerminal || observation.State == ResourceUnknown {
			return nil
		}
		executor.Conflicted = true
	} else {
		if observation.StartedAt.IsZero() && !prior.StartedAt.After(observation.ObservedAt) {
			observation.StartedAt = prior.StartedAt
		}
		if !prior.StartedAt.IsZero() && !observation.StartedAt.Equal(prior.StartedAt) {
			executor.Conflicted = true
		}
		if observation.ObservedAt.Equal(prior.ObservedAt) && observation != prior {
			executor.Conflicted = true
		}
		executor.Resource = observation
	}
	return s.saveExecutor(ctx, lease, profile, *executor, now)
}

func (s *Store) ObserveProcess(ctx context.Context, lease Lease, profile string, binding ExecutorBinding, observation ProcessObservation, now time.Time) error {
	if !observation.valid() || now.IsZero() || observation.ObservedAt.After(now) {
		return errors.New("invalid runner process observation")
	}
	observation.ObservedAt = observation.ObservedAt.UTC()
	executor, err := s.boundExecutor(ctx, lease.Deployment, profile, binding)
	if err != nil {
		return err
	}
	if executor.Runner != observation.Runner {
		return ErrExecutorIdentity
	}
	prior := executor.Process
	lateExit := observation.State == ProcessExited && prior.State != ProcessExited
	older := observation.ObservedAt.Before(prior.ObservedAt)
	if older && !lateExit {
		return nil
	}
	if older && prior.State != ProcessUnknown {
		executor.Conflicted = true
	}
	if prior.State == ProcessExited {
		if observation.State == ProcessExited || observation.State == ProcessUnknown {
			return nil
		}
		executor.Conflicted = true
	} else {
		if observation.ObservedAt.Equal(prior.ObservedAt) && observation != prior {
			executor.Conflicted = true
		}
		executor.Process = observation
	}
	return s.saveExecutor(ctx, lease, profile, *executor, now)
}

func (s *Store) boundExecutor(ctx context.Context, deployment, profile string, binding ExecutorBinding) (*Executor, error) {
	if !binding.valid() {
		return nil, ErrExecutorIdentity
	}
	executor, err := s.Executor(ctx, deployment, profile, binding.AllocationID)
	if err != nil {
		return nil, err
	}
	if executor == nil || executor.Binding != binding {
		return nil, ErrExecutorIdentity
	}
	return executor, nil
}

func (s *Store) saveExecutor(ctx context.Context, lease Lease, profile string, executor Executor, now time.Time) error {
	change, err := executor.change(lease.Deployment, profile)
	if err != nil {
		return err
	}
	// Recovery may record evidence about an existing allocation while external
	// mutations remain blocked until takeover is authorized.
	return s.applyOwned(ctx, lease, now, "", change)
}
