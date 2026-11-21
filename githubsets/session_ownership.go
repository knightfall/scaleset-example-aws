package githubsets

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

var ErrSessionUnconfirmed = errors.New("GitHub session outcome requires reconciliation")

// OpenOwnedSession creates a session for exactly the configured scale set.
// The context deadline bounds the one-attempt mutation and its receipt.
func (c *Connection) OpenOwnedSession(ctx context.Context, ledger *lifecycle.Store, lease lifecycle.Lease, profile string) (*Session, error) {
	if _, err := c.sessionDeadline(ctx, ledger, lease); err != nil {
		return nil, err
	}
	target, err := c.sessionTarget(ctx, profile)
	if err != nil {
		return nil, err
	}
	return c.openSession(ctx, ledger, lease, profile, target)
}

func (c *Connection) sessionDeadline(ctx context.Context, ledger *lifecycle.Store, lease lifecycle.Lease) (time.Time, error) {
	deadline, bounded := ctx.Deadline()
	if ledger == nil || lease.Deployment != c.deployment || lease.Phase != lifecycle.LeaseActive ||
		!bounded || !deadline.After(time.Now()) || deadline.After(lease.ValidUntil) {
		return time.Time{}, errors.New("GitHub session requires an active deployment lease and bounded context")
	}
	return deadline, nil
}

func (c *Connection) sessionTarget(ctx context.Context, profile string) (lifecycle.SessionTarget, error) {
	set, err := c.Lookup(ctx, profile)
	if err != nil {
		return lifecycle.SessionTarget{}, err
	}
	return lifecycle.SessionTarget{Scope: c.scope, ScaleSetID: set.ID}, nil
}

func (c *Connection) openSession(ctx context.Context, ledger *lifecycle.Store, lease lifecycle.Lease, profile string, target lifecycle.SessionTarget) (*Session, error) {
	deadline, err := c.sessionDeadline(ctx, ledger, lease)
	if err != nil {
		return nil, err
	}
	intent, err := ledger.BeginSessionOpen(ctx, lease, profile, target, time.Now(), deadline)
	if err != nil {
		return nil, err
	}
	raw, err := c.client.MessageSessionClient(oneWriteAttempt(ctx), target.ScaleSetID, intent.OwnerName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionUnconfirmed, requestError("open GitHub session", err, c.rateLimitKey))
	}
	session, err := AdaptSession(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionUnconfirmed, err)
	}
	response := raw.Session()
	queue, err := url.Parse(response.MessageQueueURL)
	if response.OwnerName != intent.OwnerName || response.RunnerScaleSet == nil || response.RunnerScaleSet.ID != target.ScaleSetID ||
		err != nil || queue.Scheme != "https" || queue.Hostname() == "" || queue.User != nil || response.MessageQueueAccessToken == "" {
		return nil, fmt.Errorf("%w: invalid returned session identity", ErrSessionUnconfirmed)
	}
	owner, err := ledger.ConfirmSessionOpen(ctx, lease, profile, intent, session.ID(), session.InitialDemand(), time.Now())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSessionUnconfirmed, err)
	}
	session.ownership = owner
	session.deployment = lease.Deployment
	session.profile = profile
	session.credentialVersion = c.VersionID
	session.rateLimitKey = c.rateLimitKey
	return session, nil
}

// CloseOwned closes only this session. Stop its polling lane before calling.
func (s *Session) CloseOwned(ctx context.Context, ledger *lifecycle.Store, lease lifecycle.Lease) error {
	deadline, bounded := ctx.Deadline()
	if ledger == nil || s.deployment != lease.Deployment || s.profile == "" || s.ID() != s.ownership.SessionID || !bounded {
		return errors.New("closing requires the exact owned GitHub session and a bounded context")
	}
	intent, err := ledger.BeginSessionClose(ctx, lease, s.profile, s.ownership.OwnerName, s.ID(), time.Now(), deadline)
	if err != nil {
		return err
	}
	if intent.Phase == lifecycle.SessionClosed {
		return nil
	}
	if err := s.client.Close(oneWriteAttempt(ctx)); err != nil {
		return fmt.Errorf("%w: %w", ErrSessionUnconfirmed, requestError("close GitHub session", err, s.rateLimitKey))
	}
	if err := ledger.ConfirmSessionClose(ctx, lease, s.profile, intent, time.Now()); err != nil {
		return fmt.Errorf("%w: %w", ErrSessionUnconfirmed, err)
	}
	return nil
}

// ReplaceOwnedSession checks the candidate's exact profile before closing the
// current session. A new open is possible only after durable close confirmation.
func (c *Connection) ReplaceOwnedSession(ctx context.Context, ledger *lifecycle.Store, lease lifecycle.Lease, current *Session) (*Session, error) {
	if _, err := c.sessionDeadline(ctx, ledger, lease); err != nil {
		return nil, err
	}
	if current == nil || current.deployment != c.deployment || current.profile == "" ||
		current.ownership.Target.Scope != c.scope {
		return nil, lifecycle.ErrSessionChanged
	}
	if current.credentialVersion == c.VersionID {
		owner, err := ledger.SessionOwnership(ctx, lease.Deployment, current.profile)
		if err != nil {
			return nil, err
		}
		if owner.Phase != lifecycle.SessionOpen || owner.Epoch != lease.Epoch || owner.ControllerID != lease.Owner ||
			owner.OwnerName != current.ownership.OwnerName || owner.SessionID != current.ID() {
			return nil, lifecycle.ErrSessionChanged
		}
		return current, nil
	}
	target, err := c.sessionTarget(ctx, current.profile)
	if err != nil {
		return nil, err
	}
	if current.ownership.Target != target {
		return nil, ErrRegistrationDrift
	}
	if err := current.CloseOwned(ctx, ledger, lease); err != nil {
		return nil, err
	}
	return c.openSession(ctx, ledger, lease, current.profile, target)
}
