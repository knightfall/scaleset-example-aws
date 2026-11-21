package lifecycle

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	ProfileActive   = "active"
	ProfileDraining = "draining"
	ProfileDisabled = "disabled"
	ProfileRetired  = "retired"
)

var (
	ErrConfigurationGeneration = errors.New("configuration generation is stale or does not match its digest")
	ErrConfigurationLease      = errors.New("a changed configuration requires a new lease epoch")
	ErrProfileMigration        = errors.New("profile binding changed and requires migration")
	ErrProfileRetirement       = errors.New("profile requires verified retirement before disablement or removal")
)

type DesiredProfile struct {
	ID            string
	BindingDigest string
	State         string
}

type Rollout struct {
	Generation int64
	Digest     string
	Profiles   []DesiredProfile
}

type ProfileCatalog struct {
	Generation int64                     `json:"generation"`
	Digest     string                    `json:"digest"`
	Epoch      int64                     `json:"epoch"`
	Profiles   map[string]CatalogProfile `json:"profiles"`
	revision   int64
}

type CatalogProfile struct {
	BindingDigest string              `json:"bindingDigest"`
	State         string              `json:"state"`
	RetiredAt     time.Time           `json:"retiredAt,omitempty"`
	Retirement    *RetirementProgress `json:"retirement,omitempty"`
}

func catalogKey(deployment string) Key {
	return Key{PK: "ENV#" + deployment, SK: "CATALOG"}
}

func (s *Store) Catalog(ctx context.Context, deployment string) (ProfileCatalog, error) {
	record, err := s.Get(ctx, catalogKey(deployment))
	if err != nil || record == nil {
		return ProfileCatalog{}, err
	}
	var catalog ProfileCatalog
	if err := json.Unmarshal(record.Payload, &catalog); err != nil || catalog.Generation <= 0 ||
		catalog.Epoch <= 0 || !validDigest(catalog.Digest) || catalog.Profiles == nil {
		return ProfileCatalog{}, errors.New("invalid durable profile catalog")
	}
	for _, profile := range catalog.Profiles {
		if !validDigest(profile.BindingDigest) || !validCatalogState(profile) {
			return ProfileCatalog{}, errors.New("invalid durable catalog profile")
		}
	}
	catalog.revision = record.Revision
	return catalog, nil
}

func validCatalogState(profile CatalogProfile) bool {
	switch profile.State {
	case ProfileActive, ProfileDraining:
		return profile.RetiredAt.IsZero()
	case ProfileRetired:
		return !profile.RetiredAt.IsZero()
	default:
		return false
	}
}

func validDigest(digest string) bool {
	bytes, err := hex.DecodeString(digest)
	return err == nil && len(bytes) == 32
}

func (r Rollout) desiredProfiles() (map[string]DesiredProfile, error) {
	if r.Generation <= 0 || !validDigest(r.Digest) {
		return nil, ErrConfigurationGeneration
	}
	desired := make(map[string]DesiredProfile, len(r.Profiles))
	hasEnabledProfile := false
	for _, profile := range r.Profiles {
		if profile.ID == "" || strings.Contains(profile.ID, "#") {
			return nil, errors.New("rollout requires a valid profile ID")
		}
		if _, exists := desired[profile.ID]; exists {
			return nil, errors.New("rollout repeats a profile ID")
		}
		switch profile.State {
		case ProfileActive, ProfileDraining:
			hasEnabledProfile = true
			if !validDigest(profile.BindingDigest) {
				return nil, errors.New("rollout requires a profile binding digest")
			}
		case ProfileDisabled:
		default:
			return nil, errors.New("invalid rollout profile state")
		}
		desired[profile.ID] = profile
	}
	if !hasEnabledProfile {
		return nil, errors.New("rollout requires at least one enabled profile")
	}
	return desired, nil
}

// ApplyRollout atomically activates the configuration and records tombstones.
// Requiring a fresh lease epoch for changes fences every prior effect lane.
func (s *Store) ApplyRollout(ctx context.Context, lease Lease, rollout Rollout, now time.Time) (ProfileCatalog, error) {
	desired, err := rollout.desiredProfiles()
	if err != nil {
		return ProfileCatalog{}, err
	}
	current, err := s.Catalog(ctx, lease.Deployment)
	if err != nil {
		return ProfileCatalog{}, err
	}
	if rollout.Generation < current.Generation ||
		(rollout.Generation == current.Generation && rollout.Digest != current.Digest) {
		return ProfileCatalog{}, ErrConfigurationGeneration
	}
	if lease.Phase != LeaseActive || !lease.ValidUntil.After(now) || now.IsZero() || lease.Epoch < current.Epoch {
		return ProfileCatalog{}, ErrLeaseLost
	}
	if rollout.Generation == current.Generation && current.Epoch == lease.Epoch {
		return current, nil
	}
	if rollout.Generation > current.Generation && current.Epoch == lease.Epoch {
		return ProfileCatalog{}, ErrConfigurationLease
	}

	next := ProfileCatalog{
		Generation: rollout.Generation, Digest: rollout.Digest, Epoch: lease.Epoch,
		Profiles: maps.Clone(current.Profiles), revision: current.revision,
	}
	if next.Profiles == nil {
		next.Profiles = make(map[string]CatalogProfile)
	}
	if rollout.Generation > current.Generation {
		if err := s.applyProfileChanges(ctx, lease, &next, desired, now); err != nil {
			return ProfileCatalog{}, err
		}
	}
	return s.saveCatalog(ctx, lease, next, now)
}

func (s *Store) applyProfileChanges(ctx context.Context, lease Lease, next *ProfileCatalog, desired map[string]DesiredProfile, now time.Time) error {
	for id, existing := range next.Profiles {
		if existing.State == ProfileRetired {
			continue
		}
		profile, present := desired[id]
		if present && profile.State != ProfileDisabled {
			continue
		}
		if err := s.checkRetirement(ctx, lease, id, existing, now); err != nil {
			return fmt.Errorf("profile %s: %w", id, err)
		}
		existing.State, existing.RetiredAt = ProfileRetired, now.UTC()
		next.Profiles[id] = existing
	}
	for id, profile := range desired {
		if profile.State == ProfileDisabled {
			continue
		}
		existing, present := next.Profiles[id]
		if present && (existing.State == ProfileRetired || existing.BindingDigest != profile.BindingDigest) {
			return fmt.Errorf("profile %s: %w", id, ErrProfileMigration)
		}
		if present && existing.State == profile.State {
			continue
		}
		next.Profiles[id] = CatalogProfile{BindingDigest: profile.BindingDigest, State: profile.State}
	}
	return nil
}

func (s *Store) checkRetirement(ctx context.Context, lease Lease, profile string, existing CatalogProfile, now time.Time) error {
	proof := existing.Retirement
	if existing.State != ProfileDraining || proof == nil || !proof.Ready || !proof.Last.ValidUntil.After(now) {
		return ErrProfileRetirement
	}
	deployment := lease.Deployment
	owner, err := s.SessionOwnership(ctx, deployment, profile)
	if err != nil {
		return err
	}
	if owner.Phase != "" && owner.Phase != SessionClosed {
		return ErrProfileRetirement
	}
	progress, err := s.SessionProgress(ctx, deployment, profile)
	if err != nil {
		return err
	}
	if progress.HasPendingMessage() {
		return ErrProfileRetirement
	}
	reserved, revision, err := s.readAdmissionCount(ctx, ProfileKey(deployment, profile, "ADMISSION"))
	if err != nil {
		return err
	}
	if reserved != 0 {
		return ErrProfileRetirement
	}
	if revision == 0 {
		if err := s.checkEmptyAdmission(ctx, ProfileKey(deployment, profile, "").PK); err != nil {
			return err
		}
	}
	// Removed and disabled profiles have no listener to complete the rebuild.
	if err := s.InitializeEffectTracking(ctx, lease, profile, now); err != nil {
		return err
	}
	settled, err := s.ProfileEffectsSettled(ctx, deployment, profile)
	if err != nil {
		return err
	}
	if !settled {
		return ErrProfileRetirement
	}
	return nil
}

func (s *Store) saveCatalog(ctx context.Context, lease Lease, catalog ProfileCatalog, now time.Time) (ProfileCatalog, error) {
	change, err := admissionChange(lease.Deployment, catalogKey(lease.Deployment), catalog.revision, catalog)
	if err != nil {
		return ProfileCatalog{}, err
	}
	if err := s.ApplyFenced(ctx, lease, now, "", change); err != nil {
		return ProfileCatalog{}, err
	}
	catalog.revision++
	return catalog, nil
}
