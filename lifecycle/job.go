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

// releaseCompletedRequest uses the owned session's terminal event even when
// its time is absent or ahead of the local clock. Executor cleanup needs its own proof.
func (s *Store) releaseCompletedRequest(ctx context.Context, lease Lease, profile string, now time.Time, message InboxMessage, fact JobFact) error {
	reservation, _, err := s.jobReservation(ctx, lease.Deployment, profile, fact.RequestID)
	if err != nil || !reservation.counted() {
		return err
	}
	return s.ReleaseAdmission(ctx, lease, profile, now, AcquisitionEvidence{
		RequestID: fact.RequestID, State: "terminal",
		Reference: fmt.Sprintf("completed:%s:%d:%d", message.SessionID, message.MessageID, fact.RequestID),
	})
}

func (s *Store) jobReservation(ctx context.Context, deployment, profile string, requestID int64) (admissionReservation, int64, error) {
	record, err := s.Get(ctx, acquisitionKey(deployment, profile, requestID))
	if err != nil || record == nil {
		return admissionReservation{}, 0, err
	}
	reservation, err := decodeAcquisition(record, requestID)
	return reservation, record.Revision, err
}

func (r admissionReservation) counted() bool {
	switch r.State {
	case "pending", "uncertain", "accepted":
		return true
	default:
		return false
	}
}

// The accounting changes must commit with the same delivery's demand
// observation. Until then the previous snapshot may still be in use.
func (s *Store) accountedReservations(ctx context.Context, deployment, profile string, message InboxMessage, jobs map[int64]jobObservation) ([]Change, error) {
	// Leave room for the lease fence, inbox, demand snapshot and budget.
	const maxAccounted = 96
	changes := make([]Change, 0, min(len(jobs), maxAccounted))
	for id, job := range jobs {
		if len(changes) == maxAccounted {
			break
		}
		if job.ConflictingRunner || (job.Phase != JobAssigned && job.Phase != JobStarted) {
			continue
		}
		reservation, revision, err := s.jobReservation(ctx, deployment, profile, id)
		if err != nil {
			return nil, err
		}
		if !reservation.counted() {
			continue
		}
		reservation.State = "accounted"
		reservation.DecisionRef = fmt.Sprintf("%s:%s:%d:%d", job.Phase, message.SessionID, message.MessageID, id)
		change, err := admissionChange(deployment, acquisitionKey(deployment, profile, id), revision, reservation)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	if len(changes) == 0 {
		return nil, nil
	}
	budget, err := s.releaseAdmissionBudget(ctx, deployment, profile, len(changes))
	if err != nil {
		return nil, err
	}
	return append(changes, budget), nil
}
