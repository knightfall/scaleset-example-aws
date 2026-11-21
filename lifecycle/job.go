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

// A completion releases the request's reservation. It does not authorize
// retiring an executor, whose identity or occupancy may remain unresolved.
func (s *Store) releaseCompletedRequest(ctx context.Context, lease Lease, profile string, now time.Time, message InboxMessage, fact JobFact) error {
	if fact.Phase != JobCompleted || fact.OccurredAt.IsZero() || fact.OccurredAt.After(now) {
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
		RequestID: fact.RequestID, State: "terminal",
		Reference: fmt.Sprintf("completed:%s:%d:%d", message.SessionID, message.MessageID, fact.RequestID),
	})
}
