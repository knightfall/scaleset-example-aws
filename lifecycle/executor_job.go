package lifecycle

import "context"

// executorJobChanges projects an exact runner event into its allocation in the
// same transaction as JOB. Conflicting runner IDs protect both known allocations.
func (s *Store) executorJobChanges(
	ctx context.Context, deployment, profile string, fact JobFact, previous jobObservation, conflicted bool,
) ([]Change, error) {
	if fact.RunnerID <= 0 || (fact.Phase != JobStarted && fact.Phase != JobCompleted) {
		return nil, nil
	}
	runners := []int{fact.RunnerID}
	if conflicted && previous.RunnerID != 0 && previous.RunnerID != fact.RunnerID {
		runners = append(runners, previous.RunnerID)
	}
	var changes []Change
	for _, runnerID := range runners {
		executor, err := s.executorForRunner(ctx, deployment, profile, runnerID)
		if err != nil {
			return nil, err
		}
		if executor == nil {
			continue
		}
		next := executor.withJobFact(fact, conflicted)
		if next == *executor {
			continue
		}
		change, err := next.change(deployment, profile)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func (e Executor) withJobFact(fact JobFact, conflicted bool) Executor {
	if e.Conflicted {
		return e
	}
	previous := e.Attempt
	if conflicted || (previous.RequestID != 0 && previous.RequestID != fact.RequestID) ||
		contradictoryAttemptOrder(previous, fact) {
		e.Conflicted = true
		return e
	}
	rank, previousRank := jobRank(fact.Phase), jobRank(previous.Phase)
	if rank > previousRank || (rank == previousRank && fact.OccurredAt.After(previous.OccurredAt)) {
		e.Attempt = fact
	}
	return e
}

func contradictoryAttemptOrder(previous, next JobFact) bool {
	if previous.OccurredAt.IsZero() || next.OccurredAt.IsZero() || previous.Phase == next.Phase {
		return false
	}
	if previous.Phase == JobCompleted {
		return next.OccurredAt.After(previous.OccurredAt)
	}
	return next.OccurredAt.Before(previous.OccurredAt)
}
