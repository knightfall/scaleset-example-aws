package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

func activeEffectPrefix(generation int64) string {
	return "ACTIVE-OP#" + strconv.FormatInt(generation, 10) + "#"
}

func activeEffectMarker(deployment, profile string, generation int64, id string) (Change, error) {
	key := ProfileKey(deployment, profile, activeEffectPrefix(generation)+id)
	return admissionChange(deployment, key, 0, id)
}

// UnfinishedEffects reads a page from the current profile's strongly consistent
// index and verifies each journal record. It returns no dispatch authority.
func (s *Store) UnfinishedEffects(ctx context.Context, lease Lease, profile string, after Key, now time.Time) ([]Effect, Key, error) {
	if profile == "" {
		return nil, Key{}, errors.New("unfinished effect query requires a profile")
	}
	current, err := s.currentLease(ctx, lease, now)
	if err != nil {
		return nil, Key{}, err
	}
	tracking, _, err := s.readEffectCount(ctx, lease.Deployment, profile)
	if err != nil {
		return nil, Key{}, err
	}
	if !current.EffectIndex || tracking.Epoch != current.Epoch || tracking.Generation == 0 {
		return nil, Key{}, ErrEffectTrackingPending
	}
	prefix := activeEffectPrefix(tracking.Generation)
	partition := ProfileKey(lease.Deployment, profile, "").PK
	if after != (Key{}) && (after.PK != partition || !strings.HasPrefix(after.SK, prefix)) {
		return nil, Key{}, errors.New("unfinished effect cursor belongs to another profile or index generation")
	}
	page, next, err := s.queryEffectRecords(ctx, lease.Deployment, profile, prefix, after.SK)
	if err != nil {
		return nil, Key{}, err
	}
	effects := make([]Effect, 0, len(page))
	for _, marker := range page {
		id := strings.TrimPrefix(marker.Key.SK, prefix)
		var indexed string
		if err := json.Unmarshal(marker.Payload, &indexed); err != nil || indexed != id || id == "" ||
			marker.Revision != 1 || !marker.DueAt.IsZero() || !marker.ExpiresAt.IsZero() {
			return nil, Key{}, errors.New("invalid unfinished effect marker")
		}
		effect, err := s.Effect(ctx, lease.Deployment, profile, id)
		if err != nil {
			return nil, Key{}, err
		}
		if effect == nil {
			return nil, Key{}, errors.New("unfinished effect marker has no journal record")
		}
		settled, err := effect.settled()
		if err != nil {
			return nil, Key{}, err
		}
		if settled {
			return nil, Key{}, ErrConflict
		}
		effects = append(effects, *effect)
	}
	if next != "" {
		return effects, Key{PK: partition, SK: next}, nil
	}
	if after == (Key{}) && len(effects) != *tracking.Count {
		return nil, Key{}, ErrConflict
	}
	return effects, Key{}, nil
}
