package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrSessionChanged = errors.New("delivery does not belong to the current session")

type DemandCounts struct {
	Assigned   int `json:"assigned"`
	Running    int `json:"running"`
	Registered int `json:"registered"`
	Busy       int `json:"busy"`
	Idle       int `json:"idle"`
	Acquired   int `json:"acquired"`
	Available  int `json:"available"`
}

func (d DemandCounts) valid() bool {
	return d.Running >= 0 && d.Assigned >= d.Running && d.Registered >= 0 &&
		d.Busy >= 0 && d.Idle >= 0 && d.Acquired >= 0 && d.Available >= 0
}

type DemandObservation struct {
	DemandCounts
	ObservedAt time.Time `json:"observedAt"`
	MessageID  int64     `json:"messageId"`
}

type SessionProgress struct {
	SessionID               string             `json:"sessionId"`
	LastAcknowledged        int64              `json:"lastAcknowledged"`
	PendingMessageID        int64              `json:"pendingMessageId"`
	PendingZeroMessage      bool               `json:"pendingZeroMessage,omitempty"`
	AcknowledgedZeroMessage bool               `json:"acknowledgedZeroMessage,omitempty"`
	Demand                  *DemandObservation `json:"demand,omitempty"`
	revision                int64
}

// HasPendingMessage distinguishes a real message zero from an empty cursor.
func (p SessionProgress) HasPendingMessage() bool {
	return p.PendingMessageID > 0 || p.PendingZeroMessage
}

func (p SessionProgress) acknowledged(id int64) bool {
	return (p.LastAcknowledged > 0 || p.AcknowledgedZeroMessage) && id <= p.LastAcknowledged
}

type Delivery struct {
	MessageID int64
	Stats     *DemandCounts
	Offers    []int64
	Jobs      []JobFact
}

type PendingDelivery struct {
	Receipt InboxMessage
	Offers  []int64
}

type deliveryFacts struct {
	Offers       []int64 `json:"offers,omitempty"`
	AckAttempted bool    `json:"ackAttempted"`
}

func (s *Store) SessionProgress(ctx context.Context, deployment, profile string) (SessionProgress, error) {
	var progress SessionProgress
	record, err := s.Get(ctx, ProfileKey(deployment, profile, "DEMAND"))
	if err != nil || record == nil {
		return progress, err
	}
	if err := json.Unmarshal(record.Payload, &progress); err != nil || progress.SessionID == "" ||
		progress.LastAcknowledged < 0 || progress.PendingMessageID < 0 ||
		(progress.PendingZeroMessage && progress.PendingMessageID != 0) ||
		(progress.AcknowledgedZeroMessage && progress.LastAcknowledged != 0) ||
		(progress.HasPendingMessage() && progress.acknowledged(progress.PendingMessageID)) {
		return SessionProgress{}, errors.New("invalid durable session progress")
	}
	if progress.Demand != nil && (!progress.Demand.valid() || progress.Demand.ObservedAt.IsZero()) {
		return SessionProgress{}, errors.New("invalid durable demand observation")
	}
	progress.revision = record.Revision
	return progress, nil
}

// BeginDeliverySession installs the cursor for an already owned session. The
// caller confirms prior session closure before replacing its cursor.
func (s *Store) BeginDeliverySession(ctx context.Context, lease Lease, profile, previous, session string, initial *DemandCounts, now time.Time) error {
	if session == "" || strings.Contains(session, "#") || now.IsZero() || (initial != nil && !initial.valid()) {
		return errors.New("invalid initial session or demand observation")
	}
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if progress.SessionID == session {
		return nil
	}
	if progress.SessionID != previous {
		return ErrSessionChanged
	}
	progress = newSessionProgress(progress.revision, session, initial, now)
	return s.writeProgress(ctx, lease, profile, now, progress)
}

func newSessionProgress(revision int64, session string, initial *DemandCounts, now time.Time) SessionProgress {
	progress := SessionProgress{SessionID: session, revision: revision}
	if initial != nil && initial.valid() {
		progress.Demand = &DemandObservation{DemandCounts: *initial, ObservedAt: now.UTC()}
	}
	return progress
}

// PersistDelivery saves a sanitized receipt first, then monotonic job facts,
// then the current demand and the pointer to the pending delivery. A crash
// before the final write leaves the original delivery eligible for replay.
func (s *Store) PersistDelivery(ctx context.Context, lease Lease, profile, session string, now time.Time, delivery Delivery) error {
	if err := delivery.validate(); err != nil {
		return err
	}
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if session == "" || progress.SessionID != session {
		return ErrSessionChanged
	}
	if progress.HasPendingMessage() && progress.PendingMessageID != delivery.MessageID {
		return errors.New("a different delivery is still pending")
	}
	entry, revision, err := s.readDeliveryReceipt(ctx, lease.Deployment, profile, session, now, delivery)
	if err != nil {
		return err
	}
	if entry.Delivery != nil {
		return nil
	}
	if progress.acknowledged(delivery.MessageID) {
		return errors.New("out-of-order delivery has no durable facts")
	}
	key := inboxKey(lease.Deployment, profile, entry.InboxMessage)
	if revision == 0 {
		receipt, err := admissionChange(lease.Deployment, key, 0, entry)
		if err != nil {
			return err
		}
		if err := s.ApplyFenced(ctx, lease, now, "", receipt); err != nil {
			return err
		}
		revision++
	}
	jobs := make(map[int64]jobObservation, len(delivery.Jobs))
	for _, fact := range delivery.Jobs {
		job, err := s.recordJobFact(ctx, lease, profile, now, fact)
		if err != nil {
			return err
		}
		jobs[fact.RequestID] = job
	}
	for _, job := range jobs {
		if job.ConflictingRunner || job.Phase != JobCompleted {
			continue
		}
		if err := s.releaseCompletedRequest(ctx, lease, profile, now, entry.InboxMessage, job.JobFact); err != nil {
			return err
		}
	}
	entry.Delivery = &deliveryFacts{Offers: delivery.Offers}
	processed, err := admissionChange(lease.Deployment, key, revision, entry)
	if err != nil {
		return err
	}
	progress.PendingMessageID = delivery.MessageID
	progress.PendingZeroMessage = delivery.MessageID == 0
	stats := delivery.Stats
	if stats != nil {
		// An inconsistent snapshot closes admission without dropping the job facts.
		progress.Demand = nil
		if stats.valid() {
			progress.Demand = &DemandObservation{
				DemandCounts: *stats, ObservedAt: now.UTC(), MessageID: delivery.MessageID,
			}
		}
	}
	// Aggregate statistics cannot prove that a particular reservation is included.
	// Keep reservations until request-specific evidence authorizes their release.
	return s.writeProgress(ctx, lease, profile, now, progress, processed)
}

func (d Delivery) validate() error {
	if d.MessageID < 0 {
		return errors.New("invalid delivery ID")
	}
	for _, id := range d.Offers {
		if id <= 0 {
			return errors.New("offered request IDs must be positive")
		}
	}
	for _, fact := range d.Jobs {
		if fact.RequestID <= 0 || fact.RunnerID < 0 || jobRank(fact.Phase) == 0 {
			return errors.New("invalid job observation")
		}
	}
	return nil
}

func (s *Store) readDeliveryReceipt(
	ctx context.Context, deployment, profile, session string, now time.Time, delivery Delivery,
) (inboxEntry, int64, error) {
	// Statistics are a snapshot and can change when GitHub redelivers the same message.
	stable := delivery
	stable.Stats = nil
	message, err := stable.receipt(session, now)
	if err != nil {
		return inboxEntry{}, 0, err
	}
	entry, revision, err := s.readInbox(ctx, deployment, profile, message)
	if !errors.Is(err, ErrConflict) || delivery.Stats == nil {
		return entry, revision, err
	}

	// Older receipts hashed the whole delivery. Accept an exact legacy replay.
	legacy, err := delivery.receipt(session, now)
	if err != nil {
		return inboxEntry{}, 0, err
	}
	return s.readInbox(ctx, deployment, profile, legacy)
}

func (d Delivery) receipt(session string, now time.Time) (InboxMessage, error) {
	payload, err := json.Marshal(d)
	if err != nil {
		return InboxMessage{}, fmt.Errorf("encode delivery facts: %w", err)
	}
	return normalizeInbox(InboxMessage{
		SessionID: session, MessageID: d.MessageID,
		Digest: fmt.Sprintf("%x", sha256.Sum256(payload)), ReceivedAt: now.UTC(),
	})
}

func (s *Store) PendingDelivery(ctx context.Context, deployment, profile, session string, id int64) (PendingDelivery, error) {
	entry, _, err := s.loadDelivery(ctx, deployment, profile, session, id)
	if err != nil {
		return PendingDelivery{}, err
	}
	return PendingDelivery{Receipt: entry.InboxMessage, Offers: entry.Delivery.Offers}, nil
}

func (s *Store) AuthorizeAcknowledgement(ctx context.Context, lease Lease, profile, session string, id int64, now time.Time) error {
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if progress.SessionID != session || id < 0 || !progress.HasPendingMessage() || progress.PendingMessageID != id {
		return ErrSessionChanged
	}
	entry, revision, err := s.loadDelivery(ctx, lease.Deployment, profile, session, id)
	if err != nil {
		return err
	}
	if entry.Admission == nil || !entry.Admission.OutcomeRecorded {
		return errors.New("cannot acknowledge until admission is durable")
	}
	entry.Delivery.AckAttempted = true
	change, err := admissionChange(lease.Deployment, inboxKey(lease.Deployment, profile, entry.InboxMessage), revision, entry)
	if err != nil {
		return err
	}
	return s.writeProgress(ctx, lease, profile, now, progress, change)
}

func (s *Store) ConfirmAcknowledgement(ctx context.Context, lease Lease, profile, session string, id int64, now time.Time) error {
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if progress.SessionID != session || id < 0 {
		return ErrSessionChanged
	}
	if progress.acknowledged(id) {
		return nil
	}
	if !progress.HasPendingMessage() || progress.PendingMessageID != id {
		return ErrSessionChanged
	}
	entry, _, err := s.loadDelivery(ctx, lease.Deployment, profile, session, id)
	if err != nil {
		return err
	}
	if !entry.Delivery.AckAttempted {
		return errors.New("no durable acknowledgement intent")
	}
	progress.LastAcknowledged = id
	progress.AcknowledgedZeroMessage = id == 0
	progress.PendingMessageID = 0
	progress.PendingZeroMessage = false
	return s.writeProgress(ctx, lease, profile, now, progress)
}

func (s *Store) loadDelivery(ctx context.Context, deployment, profile, session string, id int64) (inboxEntry, int64, error) {
	key := inboxKey(deployment, profile, InboxMessage{SessionID: session, MessageID: id})
	record, err := s.Get(ctx, key)
	if err != nil {
		return inboxEntry{}, 0, err
	}
	var entry inboxEntry
	if record == nil || json.Unmarshal(record.Payload, &entry) != nil || entry.Delivery == nil ||
		entry.SessionID != session || entry.MessageID != id {
		return inboxEntry{}, 0, errors.New("missing or invalid durable delivery")
	}
	if _, err := normalizeInbox(entry.InboxMessage); err != nil {
		return inboxEntry{}, 0, err
	}
	return entry, record.Revision, nil
}

func (s *Store) writeProgress(ctx context.Context, lease Lease, profile string, now time.Time, progress SessionProgress, changes ...Change) error {
	key := ProfileKey(lease.Deployment, profile, "DEMAND")
	change, err := admissionChange(lease.Deployment, key, progress.revision, progress)
	if err != nil {
		return err
	}
	return s.ApplyFenced(ctx, lease, now, "", append(changes, change)...)
}
