package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SessionOpening = "opening"
	SessionOpen    = "open"
	SessionClosing = "closing"
	SessionClosed  = "closed"
)

var ErrSessionRecoveryRequired = errors.New("GitHub session requires confirmed closure or recovery evidence")

type SessionTarget struct {
	Scope      string `json:"scope"`
	ScaleSetID int    `json:"scaleSetId"`
}

type SessionOwnership struct {
	Target       SessionTarget `json:"target"`
	ControllerID string        `json:"controllerId"`
	OwnerName    string        `json:"ownerName"`
	Epoch        int64         `json:"epoch"`
	Generation   int64         `json:"generation"`
	Phase        string        `json:"phase"`
	SessionID    string        `json:"sessionId,omitempty"`
	PreviousID   string        `json:"previousSessionId,omitempty"`
	Deadline     time.Time     `json:"deadline,omitempty"`
	ProofRef     string        `json:"proofRef,omitempty"`
	revision     int64
}

type SessionClosureProof struct {
	Target    SessionTarget
	OwnerName string
	SessionID string
	Reference string
}

func sessionOwnershipKey(deployment, profile string) Key {
	return ProfileKey(deployment, profile, "SESSION")
}

func (s *Store) SessionOwnership(ctx context.Context, deployment, profile string) (SessionOwnership, error) {
	record, err := s.Get(ctx, sessionOwnershipKey(deployment, profile))
	if err != nil || record == nil {
		return SessionOwnership{}, err
	}
	var owner SessionOwnership
	if err := json.Unmarshal(record.Payload, &owner); err != nil || !owner.valid() {
		return SessionOwnership{}, errors.New("invalid durable GitHub session ownership")
	}
	owner.revision = record.Revision
	return owner, nil
}

func (o SessionOwnership) valid() bool {
	if o.Target.Scope == "" || o.Target.ScaleSetID <= 0 || o.ControllerID == "" ||
		o.Generation <= 0 || o.Epoch <= 0 || o.OwnerName == "" ||
		strings.Contains(o.SessionID, "#") || strings.Contains(o.PreviousID, "#") {
		return false
	}
	switch o.Phase {
	case SessionOpening:
		return o.SessionID == "" && !o.Deadline.IsZero()
	case SessionOpen:
		return o.SessionID != ""
	case SessionClosing:
		return o.SessionID != "" && !o.Deadline.IsZero()
	case SessionClosed:
		return o.ProofRef != ""
	default:
		return false
	}
}

// BeginSessionOpen claims the scale set and records a one-attempt open intent.
// An unconfirmed intent continues to block even after its deadline.
func (s *Store) BeginSessionOpen(ctx context.Context, lease Lease, profile string, target SessionTarget, now, deadline time.Time) (SessionOwnership, error) {
	if lease.Deployment == "" || profile == "" || target.Scope == "" || target.ScaleSetID <= 0 ||
		!deadline.After(now) || deadline.After(lease.ValidUntil) {
		return SessionOwnership{}, errors.New("session opening requires a target and deadline within the lease")
	}
	current, err := s.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return SessionOwnership{}, err
	}
	if current.Phase != "" && (current.Phase != SessionClosed || current.Target != target) {
		return SessionOwnership{}, ErrSessionRecoveryRequired
	}
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return SessionOwnership{}, err
	}
	if progress.SessionID != current.cursorID() {
		return SessionOwnership{}, ErrSessionRecoveryRequired
	}
	if current.Generation == 1<<63-1 {
		return SessionOwnership{}, errors.New("GitHub session generation exhausted")
	}
	next := SessionOwnership{
		Target: target, ControllerID: lease.Owner, Epoch: lease.Epoch,
		Generation: current.Generation + 1, Phase: SessionOpening, Deadline: deadline.UTC(),
		PreviousID: progress.SessionID, revision: current.revision,
	}
	identity := fmt.Sprintf("%q/%q/%q/%d/%d", lease.Deployment, profile, lease.Owner, lease.Epoch, next.Generation)
	digest := sha256.Sum256([]byte(identity))
	next.OwnerName = fmt.Sprintf("runner-controller-%x", digest[:16])
	claim, err := s.sessionClaim(ctx, lease.Deployment, profile, target)
	if err != nil {
		return SessionOwnership{}, err
	}
	change, err := sessionChange(lease.Deployment, profile, next)
	if err != nil {
		return SessionOwnership{}, err
	}
	if err := s.ApplyFenced(ctx, lease, now, "", claim, change); err != nil {
		return SessionOwnership{}, err
	}
	next.revision++
	return next, nil
}

func (s *Store) ConfirmSessionOpen(ctx context.Context, lease Lease, profile string, intent SessionOwnership, sessionID string, initial *DemandCounts, now time.Time) (SessionOwnership, error) {
	if intent.Phase != SessionOpening || intent.ControllerID != lease.Owner || intent.Epoch != lease.Epoch ||
		sessionID == "" || strings.Contains(sessionID, "#") || (initial != nil && !initial.valid()) {
		return SessionOwnership{}, ErrSessionChanged
	}
	current, err := s.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return SessionOwnership{}, err
	}
	if current.revision != intent.revision || current.Phase != SessionOpening || current.OwnerName != intent.OwnerName {
		return SessionOwnership{}, ErrSessionChanged
	}
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return SessionOwnership{}, err
	}
	if progress.SessionID != current.PreviousID || sessionID == current.PreviousID {
		return SessionOwnership{}, ErrSessionChanged
	}
	progress = newSessionProgress(progress.revision, sessionID, initial, now)
	current.Phase = SessionOpen
	current.SessionID = sessionID
	current.Deadline = time.Time{}
	change, err := sessionChange(lease.Deployment, profile, current)
	if err != nil {
		return SessionOwnership{}, err
	}
	if err := s.writeProgress(ctx, lease, profile, now, progress, change); err != nil {
		return SessionOwnership{}, err
	}
	current.revision++
	return current, nil
}

func (s *Store) BeginSessionClose(ctx context.Context, lease Lease, profile, ownerName, sessionID string, now, deadline time.Time) (SessionOwnership, error) {
	if sessionID == "" || !deadline.After(now) || deadline.After(lease.ValidUntil) {
		return SessionOwnership{}, errors.New("session closing requires an identity and deadline within the lease")
	}
	current, err := s.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return SessionOwnership{}, err
	}
	if current.OwnerName != ownerName || current.SessionID != sessionID ||
		current.Epoch != lease.Epoch || current.ControllerID != lease.Owner {
		return SessionOwnership{}, ErrSessionRecoveryRequired
	}
	if current.Phase == SessionClosed {
		return current, nil
	}
	switch current.Phase {
	case SessionOpen:
	case SessionClosing:
		// Retry the exact owned DELETE only after the prior attempt window.
		if current.Deadline.After(now) {
			return SessionOwnership{}, ErrSessionRecoveryRequired
		}
	default:
		return SessionOwnership{}, ErrSessionRecoveryRequired
	}
	current.Phase = SessionClosing
	current.Deadline = deadline.UTC()
	change, err := sessionChange(lease.Deployment, profile, current)
	if err != nil {
		return SessionOwnership{}, err
	}
	if err := s.ApplyFenced(ctx, lease, now, "", change); err != nil {
		return SessionOwnership{}, err
	}
	current.revision++
	return current, nil
}

func (s *Store) ConfirmSessionClose(ctx context.Context, lease Lease, profile string, intent SessionOwnership, now time.Time) error {
	current, err := s.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if current.Phase != SessionClosing || current.revision != intent.revision ||
		current.OwnerName != intent.OwnerName || current.SessionID != intent.SessionID ||
		current.Epoch != lease.Epoch || current.ControllerID != lease.Owner {
		return ErrSessionChanged
	}
	return s.recordSessionClosed(ctx, lease, profile, current, now, "github-delete-confirmed:"+current.SessionID)
}

// ReconcileSessionClosure records verified absence of the exact remote session.
// For an ambiguous open, the evidence identifies the owner name.
func (s *Store) ReconcileSessionClosure(ctx context.Context, lease Lease, profile string, proof SessionClosureProof, now time.Time) error {
	if proof.Reference == "" {
		return ErrSessionRecoveryRequired
	}
	current, err := s.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if current.Phase == "" || current.OwnerName != proof.OwnerName ||
		current.Target != proof.Target || current.SessionID != proof.SessionID {
		return ErrSessionRecoveryRequired
	}
	switch lease.Phase {
	case LeaseActive:
		if current.ControllerID != lease.Owner || current.Epoch != lease.Epoch || current.Phase == SessionOpen {
			return ErrSessionRecoveryRequired
		}
	case LeaseRecovering:
		// The session may have been left by an epoch older than the last
		// controller. The exact target and owner name bind the closure proof.
		if current.Epoch >= lease.Epoch {
			return ErrSessionRecoveryRequired
		}
	default:
		return ErrSessionRecoveryRequired
	}
	if current.Deadline.After(now) {
		return ErrSessionRecoveryRequired
	}
	if current.Phase == SessionClosed {
		if current.ProofRef == proof.Reference {
			return nil
		}
		return ErrSessionChanged
	}
	return s.recordSessionClosed(ctx, lease, profile, current, now, proof.Reference)
}

func (s *Store) recordSessionClosed(ctx context.Context, lease Lease, profile string, current SessionOwnership, now time.Time, reference string) error {
	current.Phase = SessionClosed
	current.ProofRef = reference
	current.Deadline = time.Time{}
	change, err := sessionChange(lease.Deployment, profile, current)
	if err != nil {
		return err
	}
	progress, err := s.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if progress.SessionID != current.cursorID() {
		return ErrSessionChanged
	}
	changes := []Change{change}
	if progress.SessionID != "" {
		// Closed sessions no longer provide usable demand. Keep the durable cursor.
		progress.Demand = nil
		progressChange, err := admissionChange(lease.Deployment, ProfileKey(lease.Deployment, profile, "DEMAND"), progress.revision, progress)
		if err != nil {
			return err
		}
		changes = append(changes, progressChange)
	}
	return s.applyOwned(ctx, lease, now, "", changes...)
}

func (o SessionOwnership) cursorID() string {
	if o.SessionID != "" {
		return o.SessionID
	}
	return o.PreviousID
}

func sessionChange(deployment, profile string, owner SessionOwnership) (Change, error) {
	if !owner.valid() {
		return Change{}, errors.New("invalid session ownership transition")
	}
	change, err := admissionChange(deployment, sessionOwnershipKey(deployment, profile), owner.revision, owner)
	if owner.Phase == SessionOpening || owner.Phase == SessionClosing {
		change.Record.DueAt = owner.Deadline
	}
	return change, err
}

func (s *Store) sessionClaim(ctx context.Context, deployment, profile string, target SessionTarget) (Change, error) {
	key := ClaimKey("github-set:" + target.Scope + "#" + strconv.Itoa(target.ScaleSetID))
	record, err := s.Get(ctx, key)
	if err != nil {
		return Change{}, err
	}
	var revision int64
	if record != nil {
		var owner string
		if record.Deployment != deployment || json.Unmarshal(record.Payload, &owner) != nil || owner != profile {
			return Change{}, fmt.Errorf("%w: scale set is already claimed", ErrConflict)
		}
		revision = record.Revision
	}
	return admissionChange(deployment, key, revision, profile)
}
