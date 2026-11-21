package lifecycle

import (
	"context"
	"errors"
	"time"
)

// RetirementObservation contains one authoritative reconciliation pass. Counts
// include only work or resources still owned by the profile.
type RetirementObservation struct {
	PassID         string    `json:"passId"`
	ObservedAt     time.Time `json:"observedAt"`
	ValidUntil     time.Time `json:"validUntil"`
	GitHubComplete bool      `json:"githubComplete"`
	AWSComplete    bool      `json:"awsComplete"`
	Assigned       int       `json:"assigned"`
	Busy           int       `json:"busy"`
	Handoffs       int       `json:"handoffs"`
	Effects        int       `json:"effects"`
	Reservations   int       `json:"reservations"`
	Executors      int       `json:"executors"`
}

type RetirementProgress struct {
	FirstPass       string                `json:"firstPass,omitempty"`
	FirstObservedAt time.Time             `json:"firstObservedAt,omitempty"`
	VerifyAfter     time.Time             `json:"verifyAfter,omitempty"`
	Ready           bool                  `json:"ready"`
	Last            RetirementObservation `json:"last"`
}

func (o RetirementObservation) valid(now time.Time) bool {
	return o.PassID != "" && !o.ObservedAt.IsZero() && !o.ObservedAt.After(now) &&
		o.ValidUntil.After(o.ObservedAt) && o.Assigned >= 0 && o.Busy >= 0 && o.Handoffs >= 0 &&
		o.Effects >= 0 && o.Reservations >= 0 && o.Executors >= 0
}

func (o RetirementObservation) clear() bool {
	return o.GitHubComplete && o.AWSComplete &&
		o.Assigned == 0 && o.Busy == 0 && o.Handoffs == 0 && o.Effects == 0 && o.Reservations == 0 && o.Executors == 0
}

// RecordRetirementObservation requires two separated, distinct clear passes.
// Incomplete or occupied passes reset the proof. Old input cannot regress it.
func (s *Store) RecordRetirementObservation(
	ctx context.Context, lease Lease, generation int64, profile string, observation RetirementObservation, separation time.Duration, now time.Time,
) (RetirementProgress, error) {
	if separation <= 0 || !observation.valid(now) {
		return RetirementProgress{}, errors.New("invalid retirement observation or separation")
	}
	catalog, err := s.Catalog(ctx, lease.Deployment)
	if err != nil {
		return RetirementProgress{}, err
	}
	if generation != catalog.Generation || lease.Epoch != catalog.Epoch {
		return RetirementProgress{}, ErrConfigurationGeneration
	}
	entry, found := catalog.Profiles[profile]
	if !found || entry.State != ProfileDraining {
		return RetirementProgress{}, ErrProfileRetirement
	}
	observation.ObservedAt = observation.ObservedAt.UTC()
	observation.ValidUntil = observation.ValidUntil.UTC()
	progress, changed, err := nextRetirementProgress(entry.Retirement, observation, separation, now)
	if err != nil || !changed {
		return progress, err
	}
	entry.Retirement = &progress
	catalog.Profiles[profile] = entry
	if _, err := s.saveCatalog(ctx, lease, catalog, now); err != nil {
		return RetirementProgress{}, err
	}
	return progress, nil
}

func nextRetirementProgress(prior *RetirementProgress, observation RetirementObservation, separation time.Duration, now time.Time) (RetirementProgress, bool, error) {
	progress := RetirementProgress{Last: observation}
	if prior != nil {
		switch {
		case observation.ObservedAt.Before(prior.Last.ObservedAt):
			// Late adverse evidence inside the proof window invalidates the pair.
			if observation.clear() || prior.FirstPass == "" || observation.ObservedAt.Before(prior.FirstObservedAt) {
				return *prior, false, nil
			}
			progress.Last = prior.Last
		case observation.PassID == prior.Last.PassID || observation.ObservedAt.Equal(prior.Last.ObservedAt):
			if observation != prior.Last {
				if observation.clear() {
					return RetirementProgress{}, false, ErrConflict
				}
				return progress, true, nil
			}
			return *prior, false, nil
		default:
			if prior.FirstPass != "" && observation.PassID == prior.FirstPass {
				return RetirementProgress{}, false, ErrConflict
			}
			if observation.clear() && observation.ValidUntil.After(now) {
				progress.FirstPass = prior.FirstPass
				progress.FirstObservedAt = prior.FirstObservedAt
				progress.VerifyAfter = prior.VerifyAfter
				progress.Ready = progress.FirstPass != "" && !observation.ObservedAt.Before(progress.VerifyAfter)
			}
		}
	}
	if progress.Last.clear() && progress.Last.ValidUntil.After(now) && progress.FirstPass == "" {
		progress.FirstPass = progress.Last.PassID
		progress.FirstObservedAt = progress.Last.ObservedAt
		progress.VerifyAfter = progress.Last.ObservedAt.Add(separation)
	}
	return progress, true, nil
}
