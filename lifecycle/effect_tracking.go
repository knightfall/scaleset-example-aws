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

func effectTrackingKey(deployment, profile string) Key {
	return ProfileKey(deployment, profile, "EFFECTS")
}

// ProfileEffectsSettled uses a strongly consistent count maintained with journal
// transitions. Profiles predating tracking fall back to their base-table history.
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
	count, err = s.scanEffectCount(ctx, deployment, profile, 1)
	return count == 0 && err == nil, err
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

// InitializeEffectTracking rebuilds older tracking under the current lease.
// Epoch binding prevents a count from hiding writes by an older controller.
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
	change, err := s.changeEffectCount(ctx, lease, profile, 0)
	if err != nil {
		return err
	}
	return s.applyOwned(ctx, lease, now, "", change)
}

func (s *Store) changeEffectCount(ctx context.Context, lease Lease, profile string, delta int) (Change, error) {
	count, revision, epoch, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return Change{}, err
	}
	if revision == 0 || epoch != lease.Epoch {
		// The conditional counter creation fences concurrent first transitions.
		// Existing planned and uncertain effects must survive this migration.
		count, err = s.scanEffectCount(ctx, lease.Deployment, profile, 0)
		if err != nil {
			return Change{}, err
		}
	}
	next := count + delta
	if next < 0 {
		return Change{}, errors.New("unfinished-effect count underflow or overflow")
	}
	return admissionChange(lease.Deployment, effectTrackingKey(lease.Deployment, profile), revision, effectTracking{Version: 1, Epoch: lease.Epoch, Count: &next})
}

// stopAfter bounds read-only absence checks. Zero counts all legacy work while
// initializing the durable counter in the same transaction as its first change.
func (s *Store) scanEffectCount(ctx context.Context, deployment, profile string, stopAfter int) (int, error) {
	partition := ProfileKey(deployment, profile, "").PK
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true), Limit: aws.Int32(100), ScanIndexForward: aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": text(partition), ":prefix": text("OP#"),
		},
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := s.db.Query(ctx, input)
		if err != nil {
			return 0, fmt.Errorf("read profile effects: %w", err)
		}
		for _, item := range page.Items {
			settled, err := settledEffect(item, deployment, partition)
			if err != nil {
				return 0, err
			}
			if settled {
				continue
			}
			count++
			if count < 0 {
				return 0, errors.New("unfinished-effect count overflow")
			}
			if stopAfter > 0 && count == stopAfter {
				return count, nil
			}
		}
		if len(page.LastEvaluatedKey) == 0 {
			return count, nil
		}
		cursor := readKey(page.LastEvaluatedKey)
		if cursor.PK != partition || !strings.HasPrefix(cursor.SK, "OP#") || cursor.SK <= readKey(input.ExclusiveStartKey).SK {
			return 0, errors.New("effect history cursor did not advance within its profile")
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
