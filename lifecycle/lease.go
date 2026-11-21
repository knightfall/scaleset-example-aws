package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	LeaseActive     = "active"
	LeaseRecovering = "recovering"
	LeaseReleased   = "released"
)

var (
	ErrLeaseHeld        = errors.New("deployment lease is held")
	ErrLeaseLost        = errors.New("deployment lease was lost")
	ErrRecoveryRequired = errors.New("deployment requires takeover evidence")
)

type Lease struct {
	Deployment        string    `json:"-"`
	Revision          int64     `json:"-"`
	Owner             string    `json:"owner"`
	PreviousOwner     string    `json:"previousOwner,omitempty"`
	Epoch             int64     `json:"epoch"`
	Phase             string    `json:"phase"`
	ValidUntil        time.Time `json:"validUntil"`
	ProofRef          string    `json:"proofRef,omitempty"`
	EffectIndex       bool      `json:"effectIndex,omitempty"`
	PriorIndexedEpoch int64     `json:"priorIndexedEpoch,omitempty"`
}

type TakeoverProof struct {
	PreviousOwner     string
	Reference         string
	ProcessStopped    bool
	AWSIsolated       bool
	GitHubIsolated    bool
	EffectsReconciled bool
}

type Quiescence struct {
	SessionsClosed bool
	EffectsSettled bool
}

func leaseKey(deployment string) Key {
	return Key{PK: "ENV#" + deployment, SK: "LEASE"}
}

func decodeLease(record Record) (Lease, error) {
	var lease Lease
	if err := json.Unmarshal(record.Payload, &lease); err != nil {
		return Lease{}, errors.New("invalid deployment lease payload")
	}
	lease.Deployment, lease.Revision = record.Deployment, record.Revision
	if lease.Owner == "" || lease.Epoch < 1 || lease.ValidUntil.IsZero() {
		return Lease{}, errors.New("invalid deployment lease identity or deadline")
	}
	if lease.PriorIndexedEpoch != 0 && (!lease.EffectIndex || lease.PriorIndexedEpoch != lease.Epoch-1) {
		return Lease{}, errors.New("invalid deployment lease effect index lineage")
	}
	switch lease.Phase {
	case LeaseActive, LeaseReleased:
	case LeaseRecovering:
		if lease.PreviousOwner == "" {
			return Lease{}, errors.New("recovering lease requires its previous owner")
		}
	default:
		return Lease{}, errors.New("invalid deployment lease phase")
	}
	return lease, nil
}

func (s *Store) Lease(ctx context.Context, deployment string) (*Lease, error) {
	record, err := s.Get(ctx, leaseKey(deployment))
	if err != nil || record == nil {
		return nil, err
	}
	lease, err := decodeLease(*record)
	if err != nil {
		return nil, err
	}
	return &lease, nil
}

// AcquireLease permits writes on first startup or after clean release. An
// expired owner instead produces a recovering lease with a new epoch.
func (s *Store) AcquireLease(ctx context.Context, deployment, owner string, now time.Time, ttl time.Duration) (Lease, error) {
	until := now.Add(ttl).Truncate(time.Millisecond).UTC()
	if deployment == "" || owner == "" || now.IsZero() || !until.After(now) {
		return Lease{}, errors.New("lease requires deployment, owner and a positive duration")
	}
	next := Lease{Deployment: deployment, Owner: owner, Epoch: 1, Phase: LeaseActive, ValidUntil: until, EffectIndex: true}
	current, err := s.Lease(ctx, deployment)
	if err != nil {
		return Lease{}, err
	}
	if current == nil {
		return s.saveLease(ctx, next)
	}
	if current.Phase != LeaseReleased && current.ValidUntil.After(now) {
		return Lease{}, ErrLeaseHeld
	}
	if current.Epoch == 1<<63-1 {
		return Lease{}, errors.New("deployment lease epoch exhausted")
	}
	next.Revision, next.Epoch = current.Revision, current.Epoch+1
	if current.EffectIndex {
		next.PriorIndexedEpoch = current.Epoch
	}
	switch current.Phase {
	case LeaseReleased:
		return s.saveLease(ctx, next)
	case LeaseRecovering:
		next.PreviousOwner = current.PreviousOwner
	default:
		next.PreviousOwner = current.Owner
	}
	if owner == next.PreviousOwner {
		return Lease{}, ErrRecoveryRequired
	}
	next.Phase = LeaseRecovering
	return s.saveLease(ctx, next)
}

func (s *Store) RenewLease(ctx context.Context, lease Lease, now time.Time, ttl time.Duration) (Lease, error) {
	until := now.Add(ttl).Truncate(time.Millisecond).UTC()
	current, err := s.currentLease(ctx, lease, now)
	if err != nil {
		return Lease{}, err
	}
	if !until.After(current.ValidUntil) {
		return Lease{}, errors.New("lease renewal must extend the current deadline")
	}
	current.ValidUntil = until
	return s.saveLease(ctx, current)
}

func (s *Store) AuthorizeTakeover(ctx context.Context, lease Lease, now time.Time, proof TakeoverProof) (Lease, error) {
	current, err := s.currentLease(ctx, lease, now)
	if err != nil {
		return Lease{}, err
	}
	stoppedOrIsolated := proof.ProcessStopped || (proof.AWSIsolated && proof.GitHubIsolated)
	if current.Phase != LeaseRecovering || proof.PreviousOwner != current.PreviousOwner ||
		proof.Reference == "" || !proof.EffectsReconciled || !stoppedOrIsolated {
		return Lease{}, ErrRecoveryRequired
	}
	current.Phase, current.ProofRef = LeaseActive, proof.Reference
	return s.saveLease(ctx, current)
}

func (s *Store) ReleaseLease(ctx context.Context, lease Lease, now time.Time, proof Quiescence) error {
	current, err := s.currentLease(ctx, lease, now)
	if err != nil {
		return err
	}
	if current.Phase != LeaseActive || !proof.SessionsClosed || !proof.EffectsSettled {
		return ErrRecoveryRequired
	}
	current.Phase = LeaseReleased
	_, err = s.saveLease(ctx, current)
	return err
}

func (s *Store) currentLease(ctx context.Context, lease Lease, now time.Time) (Lease, error) {
	current, err := s.Lease(ctx, lease.Deployment)
	if err != nil {
		return Lease{}, err
	}
	if now.IsZero() || current == nil || current.Owner != lease.Owner || current.Epoch != lease.Epoch ||
		current.Phase == LeaseReleased || !current.ValidUntil.After(now) {
		return Lease{}, ErrLeaseLost
	}
	return *current, nil
}

func (s *Store) saveLease(ctx context.Context, lease Lease) (Lease, error) {
	payload, err := json.Marshal(lease)
	if err != nil {
		return Lease{}, fmt.Errorf("encode deployment lease: %w", err)
	}
	err = s.Apply(ctx, "", Change{
		Record:           Record{Key: leaseKey(lease.Deployment), Deployment: lease.Deployment, Payload: payload},
		ExpectedRevision: lease.Revision,
	})
	if err != nil {
		return Lease{}, err
	}
	lease.Revision++
	return lease, nil
}

// ApplyFenced checks ownership in the same transaction as the writes. It only
// fences DynamoDB. Provider requests still require host isolation on takeover.
func (s *Store) ApplyFenced(ctx context.Context, lease Lease, now time.Time, token string, changes ...Change) error {
	if lease.Phase != LeaseActive {
		return ErrRecoveryRequired
	}
	return s.applyOwned(ctx, lease, now, token, changes...)
}

func (s *Store) applyOwned(ctx context.Context, lease Lease, now time.Time, token string, changes ...Change) error {
	if lease.Phase != LeaseActive && lease.Phase != LeaseRecovering {
		return ErrLeaseLost
	}
	if now.IsZero() || !lease.ValidUntil.After(now) {
		return ErrLeaseLost
	}
	for _, change := range changes {
		if change.Record.Deployment != lease.Deployment {
			return errors.New("fenced changes must belong to the deployment")
		}
	}
	fence := &types.ConditionCheck{
		TableName: aws.String(s.table), Key: leaseKey(lease.Deployment).item(),
		ConditionExpression: aws.String("#owner = :owner AND #epoch = :epoch AND #phase = :phase AND #until > :now"),
		ExpressionAttributeNames: map[string]string{
			"#owner": "leaseOwner", "#epoch": "leaseEpoch", "#phase": "leasePhase", "#until": "leaseUntilMillis",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": text(lease.Owner), ":epoch": number(lease.Epoch),
			":phase": text(lease.Phase), ":now": number(now.UnixMilli()),
		},
	}
	return s.apply(ctx, token, fence, changes)
}
