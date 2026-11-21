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
	Version int   `json:"version"`
	Epoch   int64 `json:"epoch"`
	// A missing count is distinct from a verified zero.
	Count *int `json:"count"`
}

var ErrEffectTrackingPending = errors.New("unfinished-effect count rebuild is pending")

const (
	effectPageSize     = 32
	effectPagesPerPass = 2
)

type effectRebuild struct {
	Epoch int64  `json:"epoch"`
	After string `json:"after"`
	Count *int   `json:"count"`
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
	count, revision, epoch, err := s.readEffectCount(ctx, deployment, profile)
	if err != nil {
		return false, err
	}
	if revision != 0 {
		lease, err := s.Lease(ctx, deployment)
		if err != nil {
			return false, err
		}
		if lease != nil && lease.Epoch == epoch {
			return count == 0, nil
		}
	}
	var after string
	for page := 0; page < effectPagesPerPass; page++ {
		count, after, err = s.readEffectPage(ctx, deployment, profile, after)
		if err != nil || count != 0 {
			return false, err
		}
		if after == "" {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) readEffectCount(ctx context.Context, deployment, profile string) (int, int64, int64, error) {
	record, err := s.Get(ctx, effectTrackingKey(deployment, profile))
	if err != nil || record == nil {
		return 0, 0, 0, err
	}
	var tracking effectTracking
	if err := json.Unmarshal(record.Payload, &tracking); err != nil || tracking.Version != 1 || tracking.Epoch <= 0 ||
		tracking.Count == nil || *tracking.Count < 0 || !record.DueAt.IsZero() || !record.ExpiresAt.IsZero() {
		return 0, 0, 0, errors.New("invalid durable unfinished-effect count")
	}
	return *tracking.Count, record.Revision, tracking.Epoch, nil
}

// InitializeEffectTracking advances a fenced rebuild by a bounded number of
// pages. Callers can retry ErrEffectTrackingPending on a later pass.
func (s *Store) InitializeEffectTracking(ctx context.Context, lease Lease, profile string, now time.Time) error {
	if lease.Deployment == "" || profile == "" {
		return errors.New("effect tracking requires deployment and profile")
	}
	_, revision, epoch, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if revision != 0 && epoch == lease.Epoch {
		return nil
	}
	key := ProfileKey(lease.Deployment, profile, "EFFECT-REBUILD")
	rebuild, checkpointRevision, err := s.readEffectRebuild(ctx, key)
	if err != nil {
		return err
	}
	count := 0
	var after string
	if rebuild.Epoch == lease.Epoch {
		if rebuild.After == "" {
			// A concurrent initializer published the completed count.
			return ErrConflict
		}
		count, after = *rebuild.Count, rebuild.After
	}
	for page := 0; page < effectPagesPerPass; page++ {
		added, next, err := s.readEffectPage(ctx, lease.Deployment, profile, after)
		if err != nil {
			return err
		}
		count += added
		if count < 0 {
			return errors.New("unfinished-effect count overflow")
		}
		checkpoint, err := admissionChange(lease.Deployment, key, checkpointRevision, effectRebuild{
			Epoch: lease.Epoch, After: next, Count: &count,
		})
		if err != nil {
			return err
		}
		if next == "" {
			tracking, err := effectCountChange(lease, profile, revision, count)
			if err != nil {
				return err
			}
			return s.applyOwned(ctx, lease, now, "", checkpoint, tracking)
		}
		if err := s.applyOwned(ctx, lease, now, "", checkpoint); err != nil {
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
		rebuild.Count == nil || *rebuild.Count < 0 || !record.DueAt.IsZero() || !record.ExpiresAt.IsZero() ||
		(rebuild.After != "" && !strings.HasPrefix(rebuild.After, "OP#")) {
		return effectRebuild{}, 0, errors.New("invalid durable effect rebuild checkpoint")
	}
	return rebuild, record.Revision, nil
}

func (s *Store) changeEffectCount(ctx context.Context, lease Lease, profile string, now time.Time, delta int) (Change, error) {
	count, revision, epoch, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return Change{}, err
	}
	if revision == 0 || epoch != lease.Epoch {
		if err := s.InitializeEffectTracking(ctx, lease, profile, now); err != nil {
			return Change{}, err
		}
		count, revision, epoch, err = s.readEffectCount(ctx, lease.Deployment, profile)
		if err != nil {
			return Change{}, err
		}
		if revision == 0 || epoch != lease.Epoch {
			return Change{}, ErrLeaseLost
		}
	}
	next := count + delta
	if next < 0 {
		return Change{}, errors.New("unfinished-effect count underflow or overflow")
	}
	return effectCountChange(lease, profile, revision, next)
}

func effectCountChange(lease Lease, profile string, revision int64, count int) (Change, error) {
	return admissionChange(lease.Deployment, effectTrackingKey(lease.Deployment, profile), revision, effectTracking{Version: 1, Epoch: lease.Epoch, Count: &count})
}

func (s *Store) readEffectPage(ctx context.Context, deployment, profile, after string) (int, string, error) {
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}
	partition := ProfileKey(deployment, profile, "").PK
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(effectPageSize), ScanIndexForward: aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(partition), ":prefix": text("OP#"),
		},
	}
	if after != "" {
		input.ExclusiveStartKey = Key{PK: partition, SK: after}.item()
	}
	page, err := s.db.Query(ctx, input)
	if err != nil {
		return 0, "", fmt.Errorf("read profile effects: %w", err)
	}
	count := 0
	for _, item := range page.Items {
		settled, err := settledEffect(item, deployment, partition)
		if err != nil {
			return 0, "", err
		}
		if !settled {
			count++
		}
	}
	if len(page.LastEvaluatedKey) == 0 {
		return count, "", nil
	}
	cursor := readKey(page.LastEvaluatedKey)
	if cursor.PK != partition || !strings.HasPrefix(cursor.SK, "OP#") || cursor.SK <= after {
		return 0, "", errors.New("effect history cursor did not advance within its profile")
	}
	return count, cursor.SK, nil
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
