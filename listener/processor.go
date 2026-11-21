// Package listener handles one owned scale set's delivery stream.
package listener

import (
	"context"
	"errors"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

type Session interface {
	ID() string
	Poll(ctx context.Context, cursor int64, capacity int) (*lifecycle.Delivery, error)
	Acquire(ctx context.Context, ids []int64) ([]int64, error)
	Acknowledge(ctx context.Context, messageID int64) error
}

type Capacity struct {
	Maximum         int
	OutsideAssigned int
}

type Processor struct {
	Store        *lifecycle.Store
	Session      Session
	MaxDemandAge time.Duration
}

// Step handles at most one delivery on the profile's serialized effect lane.
// A nil capacity denotes unknown headroom. A zero maximum closes admission.
func (p Processor) Step(ctx context.Context, lease lifecycle.Lease, profile string, capacity *Capacity) error {
	return p.step(ctx, lease, profile, capacity, nil)
}

func (p Processor) step(ctx context.Context, lease lifecycle.Lease, profile string, capacity *Capacity, refresh func(context.Context) *Capacity) error {
	if p.Store == nil || p.Session == nil || p.MaxDemandAge <= 0 || profile == "" {
		return errors.New("delivery processor requires a store, session, profile and demand freshness limit")
	}
	if capacity != nil && (capacity.Maximum < 0 || capacity.OutsideAssigned < 0) {
		return errors.New("delivery processor requires nonnegative capacity")
	}
	if lease.Phase != lifecycle.LeaseActive || !lease.ValidUntil.After(time.Now()) {
		return lifecycle.ErrLeaseLost
	}
	ctx, cancel := context.WithDeadline(ctx, lease.ValidUntil)
	defer cancel()
	current, err := p.Store.Lease(ctx, lease.Deployment)
	if err != nil {
		return err
	}
	if current == nil || current.Owner != lease.Owner || current.Epoch != lease.Epoch ||
		current.Phase != lifecycle.LeaseActive || !current.ValidUntil.After(time.Now()) {
		return lifecycle.ErrLeaseLost
	}
	progress, err := p.Store.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if progress.SessionID == "" || p.Session.ID() != progress.SessionID {
		return lifecycle.ErrSessionChanged
	}
	owner, err := p.Store.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return err
	}
	if owner.Phase != "" && (owner.Phase != lifecycle.SessionOpen || owner.SessionID != progress.SessionID ||
		owner.ControllerID != lease.Owner || owner.Epoch != lease.Epoch) {
		return lifecycle.ErrSessionChanged
	}
	if !progress.HasPendingMessage() {
		maximum := 0
		if capacity != nil {
			maximum = capacity.Maximum
		}
		delivery, err := p.Session.Poll(ctx, progress.LastAcknowledged, maximum)
		if err != nil {
			return err
		}
		if p.Session.ID() != progress.SessionID {
			return lifecycle.ErrSessionChanged
		}
		if delivery == nil {
			return nil
		}
		if err := p.Store.PersistDelivery(ctx, lease, profile, progress.SessionID, time.Now(), *delivery); err != nil {
			return err
		}
		progress, err = p.Store.SessionProgress(ctx, lease.Deployment, profile)
		if err != nil {
			return err
		}
	}
	if p.Session.ID() != progress.SessionID {
		return lifecycle.ErrSessionChanged
	}
	if !progress.HasPendingMessage() {
		return nil
	}
	if refresh != nil {
		// Backend occupancy can change during the GitHub long poll.
		capacity = refresh(ctx)
	}
	return p.finish(ctx, lease, profile, progress, capacity)
}

func (p Processor) finish(ctx context.Context, lease lifecycle.Lease, profile string, progress lifecycle.SessionProgress, capacity *Capacity) error {
	pending, err := p.Store.PendingDelivery(ctx, lease.Deployment, profile, progress.SessionID, progress.PendingMessageID)
	if err != nil {
		return err
	}
	offer := lifecycle.AdmissionOffer{Message: pending.Receipt, RequestIDs: pending.Offers}
	now := time.Now()
	if capacity != nil && progress.Demand != nil &&
		!progress.Demand.ObservedAt.After(now) && now.Sub(progress.Demand.ObservedAt) <= p.MaxDemandAge {
		offer.EffectiveMax = capacity.Maximum
		offer.Assigned = progress.Demand.Assigned
		offer.OutsideAssigned = capacity.OutsideAssigned
	}
	decision, err := p.Store.ReserveAdmission(ctx, lease, profile, now, offer)
	if err != nil {
		return err
	}
	var acquisitionErr error
	if !decision.OutcomeRecorded {
		var accepted []int64
		if decision.Send {
			accepted, acquisitionErr = p.Session.Acquire(ctx, decision.RequestIDs)
			if acquisitionErr != nil {
				accepted = nil
			}
		}
		if p.Session.ID() != progress.SessionID {
			return lifecycle.ErrSessionChanged
		}
		if err := p.Store.RecordAdmissionOutcome(ctx, lease, profile, time.Now(), pending.Receipt, accepted); err != nil {
			return errors.Join(acquisitionErr, err)
		}
	}
	err = p.acknowledge(ctx, lease, profile, progress)
	return errors.Join(acquisitionErr, err)
}

func (p Processor) acknowledge(ctx context.Context, lease lifecycle.Lease, profile string, progress lifecycle.SessionProgress) error {
	id := progress.PendingMessageID
	if err := p.Store.AuthorizeAcknowledgement(ctx, lease, profile, progress.SessionID, id, time.Now()); err != nil {
		return err
	}
	if err := p.Session.Acknowledge(ctx, id); err != nil {
		return err
	}
	if p.Session.ID() != progress.SessionID {
		return lifecycle.ErrSessionChanged
	}
	return p.Store.ConfirmAcknowledgement(ctx, lease, profile, progress.SessionID, id, time.Now())
}
