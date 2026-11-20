package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// A DynamoDB transaction also needs the lease, inbox, and budget operations.
const maxAdmissionBatch = 97

type AdmissionOffer struct {
	Message      InboxMessage
	RequestIDs   []int64
	EffectiveMax int
	// Assigned comes from the current GitHub demand sample.
	Assigned        int
	OutsideAssigned int
}

type AdmissionDecision struct {
	RequestIDs []int64
	Send       bool
	// OutcomeRecorded covers admission. Persist the other message facts before acknowledging.
	OutcomeRecorded bool
}

type AcquisitionEvidence struct {
	RequestID int64
	State     string
	Reference string
}

type inboxEntry struct {
	InboxMessage
	Admission *admissionBatch `json:"admission,omitempty"`
}

type admissionBatch struct {
	RequestIDs      []int64 `json:"requestIds"`
	AcceptedIDs     []int64 `json:"acceptedIds,omitempty"`
	OutcomeRecorded bool    `json:"outcomeRecorded"`
}

type admissionReservation struct {
	Acquisition
	InboxKey    string `json:"inboxKey"`
	DecisionRef string `json:"decisionRef,omitempty"`
}

// ReserveAdmission atomically records one delivery's decision and reserves its
// request IDs. Send is true only for the caller that committed new reservations.
// A replay may record uncertainty but must never send the same batch again.
func (s *Store) ReserveAdmission(ctx context.Context, lease Lease, profile string, now time.Time, offer AdmissionOffer) (AdmissionDecision, error) {
	message, err := normalizeInbox(offer.Message)
	if err != nil || lease.Deployment == "" || profile == "" {
		return AdmissionDecision{}, errors.New("admission requires a valid inbox message and profile")
	}
	if offer.EffectiveMax < 0 || offer.Assigned < 0 || offer.OutsideAssigned < 0 {
		return AdmissionDecision{}, errors.New("admission capacity and occupancy must be nonnegative")
	}
	for _, id := range offer.RequestIDs {
		if id <= 0 {
			return AdmissionDecision{}, errors.New("admission requires positive request IDs")
		}
	}

	entry, inboxRevision, err := s.readInbox(ctx, lease.Deployment, profile, message)
	if err != nil {
		return AdmissionDecision{}, err
	}
	if entry.Admission != nil {
		return AdmissionDecision{RequestIDs: entry.Admission.RequestIDs, OutcomeRecorded: entry.Admission.OutcomeRecorded}, nil
	}

	budgetKey := ProfileKey(lease.Deployment, profile, "ADMISSION")
	reserved, budgetRevision, err := s.readAdmissionCount(ctx, budgetKey)
	if err != nil {
		return AdmissionDecision{}, err
	}
	if budgetRevision == 0 {
		if err := s.checkEmptyAdmission(ctx, budgetKey.PK); err != nil {
			return AdmissionDecision{}, err
		}
	}
	remaining := max(0, offer.EffectiveMax-offer.Assigned)
	remaining -= min(remaining, offer.OutsideAssigned)
	remaining -= min(remaining, reserved)
	ids, err := s.availableRequests(ctx, lease.Deployment, profile, offer.RequestIDs, min(remaining, maxAdmissionBatch))
	if err != nil {
		return AdmissionDecision{}, err
	}
	entry.Admission = &admissionBatch{RequestIDs: ids, OutcomeRecorded: len(ids) == 0}
	messageKey := inboxKey(lease.Deployment, profile, message)
	inbox, err := admissionChange(lease.Deployment, messageKey, inboxRevision, entry)
	if err != nil {
		return AdmissionDecision{}, err
	}
	budget, err := admissionChange(lease.Deployment, budgetKey, budgetRevision, reserved+len(ids))
	if err != nil {
		return AdmissionDecision{}, err
	}
	changes := []Change{inbox, budget}
	for _, id := range ids {
		reservation := admissionReservation{Acquisition: Acquisition{RequestID: id, State: "pending"}, InboxKey: messageKey.SK}
		change, err := admissionChange(lease.Deployment, acquisitionKey(lease.Deployment, profile, id), 0, reservation)
		if err != nil {
			return AdmissionDecision{}, err
		}
		changes = append(changes, change)
	}
	if err := s.ApplyFenced(ctx, lease, now, "", changes...); err != nil {
		return AdmissionDecision{}, err
	}
	return AdmissionDecision{RequestIDs: ids, Send: len(ids) > 0, OutcomeRecorded: len(ids) == 0}, nil
}

func (s *Store) availableRequests(ctx context.Context, deployment, profile string, offered []int64, limit int) ([]int64, error) {
	ids := make([]int64, 0, min(len(offered), limit))
	seen := make(map[int64]bool)
	for _, id := range offered {
		if len(ids) == limit {
			break
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		record, err := s.Get(ctx, acquisitionKey(deployment, profile, id))
		if err != nil {
			return nil, err
		}
		if record == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// RecordAdmissionOutcome persists the confirmed subset of a batch. Every
// other request remains reserved as uncertain, including after a lost response.
func (s *Store) RecordAdmissionOutcome(ctx context.Context, lease Lease, profile string, now time.Time, message InboxMessage, accepted []int64) error {
	message, err := normalizeInbox(message)
	if err != nil {
		return err
	}
	entry, revision, err := s.readInbox(ctx, lease.Deployment, profile, message)
	if err != nil {
		return err
	}
	batch := entry.Admission
	if batch == nil {
		return errors.New("admission outcome requires a durable decision")
	}
	accepted = slices.Clone(accepted)
	slices.Sort(accepted)
	if err := validateAccepted(batch.RequestIDs, accepted); err != nil {
		return err
	}
	if batch.OutcomeRecorded {
		if !slices.Equal(batch.AcceptedIDs, accepted) {
			return fmt.Errorf("%w: admission outcome differs", ErrConflict)
		}
		return nil
	}
	batch.OutcomeRecorded = true
	batch.AcceptedIDs = accepted
	messageKey := inboxKey(lease.Deployment, profile, message)
	inbox, err := admissionChange(lease.Deployment, messageKey, revision, entry)
	if err != nil {
		return err
	}
	changes := []Change{inbox}
	for _, id := range batch.RequestIDs {
		reservation, revision, err := s.readAcquisition(ctx, lease.Deployment, profile, id)
		if err != nil {
			return err
		}
		if reservation.InboxKey != messageKey.SK {
			return errors.New("durable acquisition belongs to another delivery")
		}
		if reservation.State == "accounted" || reservation.State == "terminal" {
			continue
		}
		reservation.State = "uncertain"
		if _, found := slices.BinarySearch(accepted, id); found {
			reservation.State = "accepted"
		}
		change, err := admissionChange(lease.Deployment, acquisitionKey(lease.Deployment, profile, id), revision, reservation)
		if err != nil {
			return err
		}
		changes = append(changes, change)
	}
	return s.applyOwned(ctx, lease, now, "", changes...)
}

func validateAccepted(requested, accepted []int64) error {
	for i, id := range accepted {
		if !slices.Contains(requested, id) || (i > 0 && accepted[i-1] == id) {
			return errors.New("accepted requests must be a unique subset of the durable batch")
		}
	}
	return nil
}

// ReleaseAdmission removes exactly one reservation from the budget. The
// caller supplies verified, request-specific evidence of accounting or a
// terminal disposition. An aggregate demand change cannot release a request.
func (s *Store) ReleaseAdmission(ctx context.Context, lease Lease, profile string, now time.Time, evidence AcquisitionEvidence) error {
	if (evidence.State != "accounted" && evidence.State != "terminal") || evidence.Reference == "" || evidence.RequestID <= 0 {
		return errors.New("admission release requires request-specific evidence")
	}
	current, revision, err := s.readAcquisition(ctx, lease.Deployment, profile, evidence.RequestID)
	if err != nil {
		return err
	}
	if current.State == "accounted" || current.State == "terminal" {
		if current.State != evidence.State || current.DecisionRef != evidence.Reference {
			return fmt.Errorf("%w: acquisition disposition differs", ErrConflict)
		}
		return nil
	}
	budgetKey := ProfileKey(lease.Deployment, profile, "ADMISSION")
	count, budgetRevision, err := s.readAdmissionCount(ctx, budgetKey)
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("admission budget is missing a reservation")
	}
	budget, err := admissionChange(lease.Deployment, budgetKey, budgetRevision, count-1)
	if err != nil {
		return err
	}
	current.State = evidence.State
	current.DecisionRef = evidence.Reference
	key := acquisitionKey(lease.Deployment, profile, evidence.RequestID)
	reservation, err := admissionChange(lease.Deployment, key, revision, current)
	if err != nil {
		return err
	}
	return s.applyOwned(ctx, lease, now, "", budget, reservation)
}

func (s *Store) readAcquisition(ctx context.Context, deployment, profile string, id int64) (admissionReservation, int64, error) {
	var acquisition admissionReservation
	record, err := s.Get(ctx, acquisitionKey(deployment, profile, id))
	if err != nil {
		return acquisition, 0, err
	}
	if record == nil || json.Unmarshal(record.Payload, &acquisition) != nil || acquisition.RequestID != id || acquisition.InboxKey == "" {
		return admissionReservation{}, 0, errors.New("invalid or missing durable acquisition")
	}
	switch acquisition.State {
	case "pending", "uncertain", "accepted", "accounted", "terminal":
		return acquisition, record.Revision, nil
	default:
		return admissionReservation{}, 0, errors.New("invalid durable acquisition state")
	}
}

func (s *Store) readAdmissionCount(ctx context.Context, key Key) (int, int64, error) {
	record, err := s.Get(ctx, key)
	if err != nil || record == nil {
		return 0, 0, err
	}
	var count *int
	if err := json.Unmarshal(record.Payload, &count); err != nil || count == nil || *count < 0 {
		return 0, 0, errors.New("invalid durable admission count")
	}
	return *count, record.Revision, nil
}

func (s *Store) checkEmptyAdmission(ctx context.Context, partition string) error {
	page, err := s.db.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(1),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(partition), ":prefix": text("ACQUIRE#"),
		},
	})
	if err != nil {
		return fmt.Errorf("check existing admission reservations: %w", err)
	}
	if len(page.Items) > 0 {
		return errors.New("existing acquisitions require reconciliation before initializing the admission budget")
	}
	return nil
}

func (s *Store) readInbox(ctx context.Context, deployment, profile string, message InboxMessage) (inboxEntry, int64, error) {
	record, err := s.Get(ctx, inboxKey(deployment, profile, message))
	if err != nil {
		return inboxEntry{}, 0, err
	}
	if record == nil {
		return inboxEntry{InboxMessage: message}, 0, nil
	}
	var entry inboxEntry
	if record.Deployment != deployment || json.Unmarshal(record.Payload, &entry) != nil ||
		entry.SessionID != message.SessionID || entry.MessageID != message.MessageID || entry.Digest != message.Digest {
		return inboxEntry{}, 0, fmt.Errorf("%w: inbox delivery identity or content differs", ErrConflict)
	}
	return entry, record.Revision, nil
}

func admissionChange(deployment string, key Key, revision int64, value any) (Change, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return Change{}, fmt.Errorf("encode admission record: %w", err)
	}
	return Change{
		Record: Record{Deployment: deployment, Key: key, Payload: payload}, ExpectedRevision: revision,
	}, nil
}
