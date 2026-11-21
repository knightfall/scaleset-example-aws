package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	JobAssigned  = "assigned"
	JobStarted   = "started"
	JobCompleted = "completed"
)

type JobFact struct {
	RequestID  int64     `json:"requestId"`
	Phase      string    `json:"phase"`
	RunnerID   int       `json:"runnerId,omitempty"`
	OccurredAt time.Time `json:"occurredAt"`
}

type jobObservation struct {
	JobFact
	ConflictingRunner bool `json:"conflictingRunner,omitempty"`
}

func jobKey(deployment, profile string, id int64) Key {
	return ProfileKey(deployment, profile, "JOB#"+strconv.FormatInt(id, 10))
}

func jobRank(phase string) int {
	switch phase {
	case JobAssigned:
		return 1
	case JobStarted:
		return 2
	case JobCompleted:
		return 3
	default:
		return 0
	}
}

func (s *Store) recordJobFact(ctx context.Context, lease Lease, profile string, now time.Time, fact JobFact) (jobObservation, error) {
	key := jobKey(lease.Deployment, profile, fact.RequestID)
	record, err := s.Get(ctx, key)
	if err != nil {
		return jobObservation{}, err
	}
	var revision int64
	var current jobObservation
	if record != nil {
		revision = record.Revision
		if err := json.Unmarshal(record.Payload, &current); err != nil ||
			current.RequestID != fact.RequestID || current.RunnerID < 0 || jobRank(current.Phase) == 0 {
			return jobObservation{}, errors.New("invalid durable job observation")
		}
	}
	next := current.merge(fact)
	if next == current {
		return current, nil
	}
	change, err := admissionChange(lease.Deployment, key, revision, next)
	if err != nil {
		return jobObservation{}, err
	}
	if err := s.ApplyFenced(ctx, lease, now, "", change); err != nil {
		return jobObservation{}, err
	}
	return next, nil
}

func (j jobObservation) merge(fact JobFact) jobObservation {
	if j.RunnerID > 0 && fact.RunnerID > 0 && j.RunnerID != fact.RunnerID {
		j.ConflictingRunner = true
	}
	knownRunner := j.RunnerID
	if knownRunner == 0 {
		knownRunner = fact.RunnerID
	}
	newRank, currentRank := jobRank(fact.Phase), jobRank(j.Phase)
	if newRank > currentRank || (newRank == currentRank && fact.OccurredAt.After(j.OccurredAt)) {
		j.JobFact = fact
	}
	if j.RunnerID == 0 {
		j.RunnerID = knownRunner
	}
	return j
}

// Admission counts unreflected requests separately from the current assigned
// total. Update only a request identified by the owned session's job facts.
func (s *Store) reconcileJobReservation(
	ctx context.Context, lease Lease, profile string, now time.Time, message InboxMessage, fact JobFact, stats *DemandCounts,
) error {
	var disposition string
	switch fact.Phase {
	case JobCompleted:
		// A terminal queue event releases admission even when its wall clock
		// differs from ours. Executor cleanup requires its own occupancy proof.
		disposition = "terminal"
	case JobAssigned, JobStarted:
		if stats == nil || !stats.valid() || stats.Assigned == 0 {
			return nil
		}
		// The same delivery supplies the identified assignment and a current
		// assigned total. An aggregate change alone identifies no reservation.
		disposition = "accounted"
	default:
		return nil
	}
	key := acquisitionKey(lease.Deployment, profile, fact.RequestID)
	record, err := s.Get(ctx, key)
	if err != nil || record == nil {
		return err
	}
	var reservation admissionReservation
	if err := json.Unmarshal(record.Payload, &reservation); err != nil || reservation.RequestID != fact.RequestID || reservation.InboxKey == "" {
		return errors.New("invalid durable acquisition for completed request")
	}
	if reservation.State == "accounted" || reservation.State == "terminal" || reservation.State == "unstarted" {
		return nil
	}
	return s.ReleaseAdmission(ctx, lease, profile, now, AcquisitionEvidence{
		RequestID: fact.RequestID, State: disposition,
		Reference: fmt.Sprintf("%s:%s:%d:%d", fact.Phase, message.SessionID, message.MessageID, fact.RequestID),
	})
}
