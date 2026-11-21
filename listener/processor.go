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
	Store            *lifecycle.Store
	Session          Session
	MaxDemandAge     time.Duration
	pollTimeout      time.Duration
	operationTimeout time.Duration
}

// Step handles at most one delivery on the profile's serialized effect lane.
// A nil capacity denotes unknown headroom. A zero maximum closes admission.
func (p Processor) Step(ctx context.Context, lease lifecycle.Lease, profile string, capacity *Capacity) error {
	return p.step(ctx, lease, profile, capacity, nil)
}

func (p Processor) step(ctx context.Context, lease lifecycle.Lease, profile string, capacity *Capacity, refresh func(context.Context, lifecycle.Lease) *Capacity) error {
	if p.Store == nil || p.Session == nil || p.MaxDemandAge <= 0 || profile == "" {
		return errors.New("delivery processor requires a store, session, profile and demand freshness limit")
	}
	if capacity != nil && (capacity.Maximum < 0 || capacity.OutsideAssigned < 0) {
		return errors.New("delivery processor requires nonnegative capacity")
	}
	current, err := p.currentLease(ctx, lease)
	if err != nil {
		return err
	}
	request, cancel := p.operationContext(ctx, current)
	progress, err := p.progress(request, current, profile)
	cancel()
	if err != nil {
		return err
	}

	var delivery *lifecycle.Delivery
	if !progress.HasPendingMessage() {
		maximum := 0
		if capacity != nil {
			maximum = capacity.Maximum
		}
		poll, cancel := p.pollContext(ctx, current)
		delivery, err = p.Session.Poll(poll, progress.LastAcknowledged, maximum)
		cancel()
		if err != nil || delivery == nil {
			return err
		}
		if p.Session.ID() != progress.SessionID {
			return lifecycle.ErrSessionChanged
		}
		current, err = p.currentLease(ctx, lease)
		if err != nil {
			return err
		}
	}

	work, cancel := p.operationContext(ctx, current)
	defer cancel()
	if delivery != nil {
		if err := p.Store.PersistDelivery(work, current, profile, progress.SessionID, time.Now(), *delivery); err != nil {
			return err
		}
		progress, err = p.Store.SessionProgress(work, current.Deployment, profile)
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
		capacity = refresh(work, current)
	}
	return p.finish(work, current, profile, progress, capacity)
}

func (p Processor) currentLease(ctx context.Context, expected lifecycle.Lease) (lifecycle.Lease, error) {
	if expected.Phase != lifecycle.LeaseActive {
		return lifecycle.Lease{}, lifecycle.ErrLeaseLost
	}
	if p.operationTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.operationTimeout)
		defer cancel()
	}
	current, err := p.Store.Lease(ctx, expected.Deployment)
	if err != nil {
		return lifecycle.Lease{}, err
	}
	if current == nil || current.Owner != expected.Owner || current.Epoch != expected.Epoch ||
		current.Phase != lifecycle.LeaseActive || !current.ValidUntil.After(time.Now()) {
		return lifecycle.Lease{}, lifecycle.ErrLeaseLost
	}
	return *current, nil
}

func (p Processor) progress(ctx context.Context, lease lifecycle.Lease, profile string) (lifecycle.SessionProgress, error) {
	progress, err := p.Store.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return lifecycle.SessionProgress{}, err
	}
	if progress.SessionID == "" || p.Session.ID() != progress.SessionID {
		return lifecycle.SessionProgress{}, lifecycle.ErrSessionChanged
	}
	owner, err := p.Store.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return lifecycle.SessionProgress{}, err
	}
	if owner.Phase != "" && (owner.Phase != lifecycle.SessionOpen || owner.SessionID != progress.SessionID ||
		owner.ControllerID != lease.Owner || owner.Epoch != lease.Epoch) {
		return lifecycle.SessionProgress{}, lifecycle.ErrSessionChanged
	}
	return progress, nil
}

func (p Processor) operationContext(ctx context.Context, lease lifecycle.Lease) (context.Context, context.CancelFunc) {
	if p.operationTimeout > 0 {
		return boundedContext(ctx, lease.ValidUntil, p.operationTimeout)
	}
	return context.WithDeadline(ctx, lease.ValidUntil)
}

func (p Processor) pollContext(ctx context.Context, lease lifecycle.Lease) (context.Context, context.CancelFunc) {
	if p.pollTimeout > 0 {
		// The supervisor cancels this context if it cannot renew the lease.
		// Check the durable epoch and the latest deadline before handling a delivery.
		return context.WithTimeout(ctx, p.pollTimeout)
	}
	return context.WithDeadline(ctx, lease.ValidUntil)
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
