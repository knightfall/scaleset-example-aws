package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type effectTracking struct {
	Version    int   `json:"version"`
	Epoch      int64 `json:"epoch"`
	Generation int64 `json:"indexGeneration,omitempty"`
	// A missing count is distinct from a verified zero.
	Count *int `json:"count"`
}

var ErrEffectTrackingPending = errors.New("unfinished-effect count rebuild is pending")

const (
	effectPageSize     = 32
	effectPagesPerPass = 2
)

type effectRebuild struct {
	Epoch      int64  `json:"epoch"`
	Generation int64  `json:"indexGeneration,omitempty"`
	After      string `json:"after"`
	Count      *int   `json:"count"`
}

func effectTrackingKey(deployment, profile string) Key {
	return ProfileKey(deployment, profile, "EFFECTS")
}

// ProfileEffectsSettled uses a strongly consistent count maintained with journal
// transitions. Without a current count, a bounded read can prove a small
// history clear. An incomplete read keeps the profile unsettled.
func (s *Store) ProfileEffectsSettled(ctx context.Context, deployment, profile string) (bool, error) {
	if deployment == "" || profile == "" {
		return false, errors.New("effect inspection requires deployment and profile")
	}
	tracking, revision, err := s.readEffectCount(ctx, deployment, profile)
	if err != nil {
		return false, err
	}
	if revision != 0 {
		lease, err := s.Lease(ctx, deployment)
		if err != nil {
			return false, err
		}
		if lease != nil && lease.Epoch == tracking.Epoch {
			return *tracking.Count == 0, nil
		}
	}
	var after string
	for page := 0; page < effectPagesPerPass; page++ {
		active, next, err := s.readEffectPage(ctx, deployment, profile, after)
		if err != nil || len(active) != 0 {
			return false, err
		}
		after = next
		if after == "" {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) readEffectCount(ctx context.Context, deployment, profile string) (effectTracking, int64, error) {
	record, err := s.Get(ctx, effectTrackingKey(deployment, profile))
	if err != nil || record == nil {
		return effectTracking{}, 0, err
	}
	var tracking effectTracking
	if err := json.Unmarshal(record.Payload, &tracking); err != nil || tracking.Version != 1 || tracking.Epoch <= 0 ||
		tracking.Generation < 0 || tracking.Generation > tracking.Epoch || tracking.Count == nil || *tracking.Count < 0 ||
		!record.DueAt.IsZero() || !record.ExpiresAt.IsZero() {
		return effectTracking{}, 0, errors.New("invalid durable unfinished-effect count")
	}
	return tracking, record.Revision, nil
}

// InitializeEffectTracking advances a fenced rebuild by a bounded number of
// pages. Callers can retry ErrEffectTrackingPending on a later pass.
func (s *Store) InitializeEffectTracking(ctx context.Context, lease Lease, profile string, now time.Time) error {
	if lease.Deployment == "" || profile == "" {
		return errors.New("effect tracking requires deployment and profile")
	}
	tracking, revision, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if tracking.Epoch == lease.Epoch && tracking.Generation > 0 {
		return nil
	}
	if tracking.Generation > 0 && tracking.Epoch == lease.PriorIndexedEpoch {
		current, err := s.currentLease(ctx, lease, now)
		if err != nil {
			return err
		}
		if current.EffectIndex && current.PriorIndexedEpoch == tracking.Epoch {
			change, err := effectCountChange(current, profile, revision, tracking.Generation, *tracking.Count)
			if err != nil {
				return err
			}
			return s.applyOwned(ctx, current, now, "", change)
		}
	}
	key := ProfileKey(lease.Deployment, profile, "EFFECT-REBUILD")
	rebuild, checkpointRevision, err := s.readEffectRebuild(ctx, key)
	if err != nil {
		return err
	}
	count := 0
	var after string
	if rebuild.Epoch == lease.Epoch && rebuild.Generation == lease.Epoch {
		if rebuild.After == "" {
			// A concurrent initializer published the completed count.
			return ErrConflict
		}
		count, after = *rebuild.Count, rebuild.After
	}
	for page := 0; page < effectPagesPerPass; page++ {
		active, next, err := s.readEffectPage(ctx, lease.Deployment, profile, after)
		if err != nil {
			return err
		}
		count += len(active)
		if count < 0 {
			return errors.New("unfinished-effect count overflow")
		}
		checkpoint, err := admissionChange(lease.Deployment, key, checkpointRevision, effectRebuild{
			Epoch: lease.Epoch, Generation: lease.Epoch, After: next, Count: &count,
		})
		if err != nil {
			return err
		}
		changes := []Change{checkpoint}
		for _, id := range active {
			marker, err := activeEffectMarker(lease.Deployment, profile, lease.Epoch, id)
			if err != nil {
				return err
			}
			changes = append(changes, marker)
		}
		if next == "" {
			tracking, err := effectCountChange(lease, profile, revision, lease.Epoch, count)
			if err != nil {
				return err
			}
			return s.applyOwned(ctx, lease, now, "", append(changes, tracking)...)
		}
		if err := s.applyOwned(ctx, lease, now, "", changes...); err != nil {
			return err
		}
		checkpointRevision++
		after = next
	}
	return ErrEffectTrackingPending
}

func (s *Store) readEffectRebuild(ctx context.Context, key Key) (effectRebuild, int64, error) {
	record, err := s.Get(ctx, key)
	if err != nil || record == nil {
		return effectRebuild{}, 0, err
	}
	var rebuild effectRebuild
	if err := json.Unmarshal(record.Payload, &rebuild); err != nil || rebuild.Epoch <= 0 ||
		(rebuild.Generation != 0 && rebuild.Generation != rebuild.Epoch) ||
		rebuild.Count == nil || *rebuild.Count < 0 || !record.DueAt.IsZero() || !record.ExpiresAt.IsZero() ||
		(rebuild.After != "" && !strings.HasPrefix(rebuild.After, "OP#")) {
		return effectRebuild{}, 0, errors.New("invalid durable effect rebuild checkpoint")
	}
	return rebuild, record.Revision, nil
}

func (s *Store) effectMembershipChanges(ctx context.Context, lease Lease, profile, id string, now time.Time, delta int) ([]Change, error) {
	tracking, revision, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return nil, err
	}
	if tracking.Epoch != lease.Epoch || tracking.Generation == 0 {
		if err := s.InitializeEffectTracking(ctx, lease, profile, now); err != nil {
			return nil, err
		}
		tracking, revision, err = s.readEffectCount(ctx, lease.Deployment, profile)
		if err != nil {
			return nil, err
		}
		if tracking.Epoch != lease.Epoch || tracking.Generation == 0 {
			return nil, ErrLeaseLost
		}
	}
	next := *tracking.Count + delta
	if next < 0 {
		return nil, errors.New("unfinished-effect count underflow or overflow")
	}
	counter, err := effectCountChange(lease, profile, revision, tracking.Generation, next)
	if err != nil {
		return nil, err
	}
	marker, err := activeEffectMarker(lease.Deployment, profile, tracking.Generation, id)
	if err != nil {
		return nil, err
	}
	if delta < 0 {
		marker.remove, marker.ExpectedRevision = true, 1
	}
	return []Change{counter, marker}, nil
}

func effectCountChange(lease Lease, profile string, revision, generation int64, count int) (Change, error) {
	return admissionChange(lease.Deployment, effectTrackingKey(lease.Deployment, profile), revision, effectTracking{
		Version: 1, Epoch: lease.Epoch, Generation: generation, Count: &count,
	})
}

func (s *Store) readEffectPage(ctx context.Context, deployment, profile, after string) ([]string, string, error) {
	page, next, err := s.queryEffectRecords(ctx, deployment, profile, "OP#", after)
	if err != nil {
		return nil, "", err
	}
	var active []string
	for _, record := range page {
		effect, err := decodeEffect(record, strings.TrimPrefix(record.Key.SK, "OP#"))
		if err != nil {
			return nil, "", err
		}
		settled, err := effect.settled()
		if err != nil {
			return nil, "", err
		}
		if !settled {
			if !record.ExpiresAt.IsZero() {
				return nil, "", errors.New("unfinished effect cannot expire during index rebuild")
			}
			active = append(active, effect.ID)
		}
	}
	return active, next, nil
}

func (s *Store) queryEffectRecords(ctx context.Context, deployment, profile, prefix, after string) ([]Record, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	partition := ProfileKey(deployment, profile, "").PK
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(effectPageSize), ScanIndexForward: aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(partition), ":prefix": text(prefix),
		},
	}
	if after != "" {
		input.ExclusiveStartKey = Key{PK: partition, SK: after}.item()
	}
	page, err := s.db.Query(ctx, input)
	if err != nil {
		return nil, "", fmt.Errorf("read profile effects: %w", err)
	}
	records := make([]Record, 0, len(page.Items))
	prior := after
	for _, item := range page.Items {
		record, err := readRecord(item)
		if err != nil {
			return nil, "", err
		}
		if record.Deployment != deployment || record.Key.PK != partition ||
			!strings.HasPrefix(record.Key.SK, prefix) || record.Key.SK <= prior {
			return nil, "", errors.New("invalid or unordered profile effect record")
		}
		records = append(records, record)
		prior = record.Key.SK
	}
	if len(page.LastEvaluatedKey) == 0 {
		return records, "", nil
	}
	cursor := readKey(page.LastEvaluatedKey)
	if cursor.PK != partition || !strings.HasPrefix(cursor.SK, prefix) || cursor.SK <= after || cursor.SK < prior {
		return nil, "", errors.New("effect cursor did not advance within its profile")
	}
	return records, cursor.SK, nil
}

func (effect Effect) settled() (bool, error) {
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
