package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	EffectPlanned   = "planned"
	EffectExecuting = "executing"
	EffectUncertain = "uncertain"
	EffectConfirmed = "confirmed"
	EffectAbandoned = "abandoned"
	EffectCancelled = "cancelled-before-dispatch"
)

var ErrEffectNotReady = errors.New("effect cannot proceed from its durable state")

type Effect struct {
	ID            string    `json:"id"`
	Kind          string    `json:"kind"`
	Target        string    `json:"target"`
	RequestSHA256 string    `json:"requestSHA256"`
	Deadline      time.Time `json:"deadline"`
	State         string    `json:"state"`
	Epoch         int64     `json:"epoch"`
	Evidence      string    `json:"evidence,omitempty"`
	Revision      int64     `json:"-"`
}

func effectKey(deployment, profile, id string) Key {
	return ProfileKey(deployment, profile, "OP#"+id)
}

func (s *Store) Effect(ctx context.Context, deployment, profile, id string) (*Effect, error) {
	record, err := s.Get(ctx, effectKey(deployment, profile, id))
	if err != nil || record == nil {
		return nil, err
	}
	var effect Effect
	if err := json.Unmarshal(record.Payload, &effect); err != nil || effect.ID != id {
		return nil, errors.New("invalid durable effect record")
	}
	effect.Revision = record.Revision
	return &effect, nil
}

func (s *Store) PlanEffect(ctx context.Context, lease Lease, profile string, effect Effect, now time.Time) (Effect, error) {
	digest, err := hex.DecodeString(effect.RequestSHA256)
	if profile == "" || effect.ID == "" || strings.Contains(effect.ID, "#") || effect.Kind == "" ||
		effect.Target == "" || err != nil || len(digest) != sha256.Size {
		return Effect{}, errors.New("effect requires profile, identity, target and request digest")
	}
	if !effect.Deadline.After(now) || effect.Deadline.After(lease.ValidUntil) {
		return Effect{}, errors.New("effect deadline must be after now and within the lease")
	}
	effect.RequestSHA256 = hex.EncodeToString(digest)
	effect.State, effect.Epoch, effect.Revision, effect.Evidence = EffectPlanned, lease.Epoch, 0, ""
	written, err := s.saveEffect(ctx, lease, profile, effect, now)
	if !errors.Is(err, ErrConflict) {
		return written, err
	}
	existing, err := s.Effect(ctx, lease.Deployment, profile, effect.ID)
	if err != nil {
		return Effect{}, err
	}
	if existing == nil || existing.Kind != effect.Kind || existing.Target != effect.Target ||
		existing.RequestSHA256 != effect.RequestSHA256 {
		return Effect{}, ErrConflict
	}
	if existing.State == EffectCancelled {
		// A cancelled intent never reached executing and can be planned again.
		effect.Revision = existing.Revision
		return s.saveEffect(ctx, lease, profile, effect, now)
	}
	if !existing.Deadline.Equal(effect.Deadline) {
		return Effect{}, ErrConflict
	}
	return *existing, nil
}

// CancelPlannedEffect settles an intent that never started. The revision
// check prevents cancelling an intent that concurrently reaches executing.
func (s *Store) CancelPlannedEffect(ctx context.Context, lease Lease, profile, id string, now time.Time) (bool, error) {
	effect, err := s.Effect(ctx, lease.Deployment, profile, id)
	if err != nil {
		return false, err
	}
	if effect == nil || effect.State != EffectPlanned {
		return false, nil
	}
	effect.State = EffectCancelled
	_, err = s.settleEffect(ctx, lease, profile, *effect, now)
	return err == nil, err
}

// BeginEffect persists execution before a caller issues its provider request.
func (s *Store) BeginEffect(ctx context.Context, lease Lease, profile, id string, now time.Time) (Effect, error) {
	effect, err := s.Effect(ctx, lease.Deployment, profile, id)
	if err != nil {
		return Effect{}, err
	}
	if effect == nil || effect.State != EffectPlanned || !effect.Deadline.After(now) ||
		effect.Deadline.After(lease.ValidUntil) {
		return Effect{}, ErrEffectNotReady
	}
	effect.State, effect.Epoch = EffectExecuting, lease.Epoch
	return s.saveEffect(ctx, lease, profile, *effect, now)
}

func (s *Store) MarkEffectUncertain(ctx context.Context, lease Lease, profile, id string, now time.Time) (Effect, error) {
	effect, err := s.Effect(ctx, lease.Deployment, profile, id)
	if err != nil {
		return Effect{}, err
	}
	if effect == nil || effect.State != EffectExecuting || effect.Epoch != lease.Epoch {
		return Effect{}, ErrEffectNotReady
	}
	effect.State = EffectUncertain
	return s.saveEffect(ctx, lease, profile, *effect, now)
}

// ResolveEffect records provider evidence. A recovering lease can record an
// observation but cannot create an intent or start a provider request.
func (s *Store) ResolveEffect(
	ctx context.Context, lease Lease, profile, id string, now time.Time, applied bool, evidence string,
) (Effect, error) {
	if evidence == "" {
		return Effect{}, errors.New("effect resolution requires provider evidence")
	}
	effect, err := s.Effect(ctx, lease.Deployment, profile, id)
	if err != nil {
		return Effect{}, err
	}
	if effect == nil || (effect.State != EffectExecuting && effect.State != EffectUncertain) {
		return Effect{}, ErrEffectNotReady
	}
	if !applied && effect.Deadline.After(now) {
		return Effect{}, ErrEffectNotReady
	}
	effect.State, effect.Evidence = EffectConfirmed, evidence
	if !applied {
		effect.State = EffectAbandoned
	}
	return s.settleEffect(ctx, lease, profile, *effect, now)
}

func (s *Store) settleEffect(ctx context.Context, lease Lease, profile string, effect Effect, now time.Time) (Effect, error) {
	change, err := effect.change(lease.Deployment, profile, time.Time{})
	if err != nil {
		return Effect{}, err
	}
	if err := s.applyOwned(ctx, lease, now, "", change); err != nil {
		return Effect{}, err
	}
	effect.Revision++
	return effect, nil
}

func (s *Store) saveEffect(ctx context.Context, lease Lease, profile string, effect Effect, now time.Time) (Effect, error) {
	due := effect.Deadline
	if effect.State == EffectPlanned {
		due = now
	}
	change, err := effect.change(lease.Deployment, profile, due)
	if err != nil {
		return Effect{}, err
	}
	if err := s.ApplyFenced(ctx, lease, now, "", change); err != nil {
		return Effect{}, err
	}
	effect.Revision++
	return effect, nil
}

func (e Effect) change(deployment, profile string, due time.Time) (Change, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return Change{}, fmt.Errorf("encode effect: %w", err)
	}
	return Change{
		Record: Record{
			Key: effectKey(deployment, profile, e.ID), Deployment: deployment, Payload: payload, DueAt: due,
		},
		ExpectedRevision: e.Revision,
	}, nil
}

// ProfileEffectsSettled checks every effect for this profile on the base table.
// The due index can omit a recent write or an intent whose deadline has not arrived.
func (s *Store) ProfileEffectsSettled(ctx context.Context, deployment, profile string) (bool, error) {
	if deployment == "" || profile == "" {
		return false, errors.New("effect inspection requires deployment and profile")
	}
	pk := ProfileKey(deployment, profile, "").PK
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(100),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(pk), ":prefix": text("OP#"),
		},
	}
	for {
		page, err := s.db.Query(ctx, input)
		if err != nil {
			return false, fmt.Errorf("read profile effects: %w", err)
		}
		for _, item := range page.Items {
			settled, err := settledEffect(item, deployment, pk)
			if err != nil || !settled {
				return false, err
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			return true, nil
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
}

func settledEffect(item map[string]types.AttributeValue, deployment, partition string) (bool, error) {
	record, err := readRecord(item)
	if err != nil {
		return false, err
	}
	var effect Effect
	if record.Deployment != deployment || record.Key.PK != partition ||
		json.Unmarshal(record.Payload, &effect) != nil || effect.ID == "" || record.Key.SK != "OP#"+effect.ID {
		return false, errors.New("invalid durable profile effect")
	}
	switch effect.State {
	case EffectCancelled:
		return true, nil
	case EffectConfirmed, EffectAbandoned:
		return effect.Evidence != "", nil
	case EffectPlanned, EffectExecuting, EffectUncertain:
		return false, nil
	default:
		return false, errors.New("invalid durable effect state")
	}
}
