package listener

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type profileLane struct {
	supervisor  Supervisor
	artifact    runtimeconfig.Artifact
	profile     runtimeconfig.Profile
	held        *atomic.Pointer[lifecycle.Lease]
	credentials *credentialState
	session     *githubsets.Session
	version     string
}

type credentialState struct {
	mu         sync.Mutex
	connection *githubsets.Connection
	checkAt    time.Time
	cooldown   *credentialCooldown
}

func (c *credentialState) refresh(ctx context.Context, lane *profileLane, lease lifecycle.Lease) (*githubsets.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Now().Before(c.checkAt) || time.Now().Before(c.cooldown.deadline()) {
		return c.connection, nil
	}
	s := lane.supervisor
	request, cancel := boundedContext(ctx, lease.ValidUntil, s.Timing.OperationTimeout)
	defer cancel()
	candidate, err := s.Loader.Load(request, lane.artifact, lane.profile.GitHub.Connection)
	if err == nil && c.connection != nil && c.connection.VersionID != candidate.VersionID {
		err = candidate.Validate(request)
	}
	if err != nil {
		c.cooldown.record(err)
		c.checkAt = time.Now().Add(min(s.Timing.CredentialInterval, time.Second))
		return c.connection, err
	}
	if c.connection == nil || c.connection.VersionID != candidate.VersionID {
		c.connection = candidate
	}
	c.checkAt = time.Now().Add(s.Timing.CredentialInterval)
	return c.connection, nil
}

func (l *profileLane) run(ctx context.Context) error {
	var retryAt time.Time
	failures := 0
	for {
		deadline := l.credentials.cooldown.deadline()
		if retryAt.After(deadline) {
			deadline = retryAt
		}
		if !waitUntil(ctx, deadline) {
			break
		}
		err := l.step(ctx)
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			l.credentials.cooldown.record(err)
			l.supervisor.Logger.Warn("profile listener retry", "profile", l.profile.ID, "error", err)
			retryAt = time.Now().Add(retryDelay(failures))
			failures = min(failures+1, 8)
			continue
		}
		failures = 0
		retryAt = time.Now().Add(100 * time.Millisecond)
	}
	if l.session == nil {
		return nil
	}
	lease := *l.held.Load()
	closeCtx, cancel := boundedContext(context.WithoutCancel(ctx), lease.ValidUntil, l.supervisor.Timing.ShutdownTimeout)
	defer cancel()
	if err := l.session.CloseOwned(closeCtx, l.supervisor.Store, lease); err != nil {
		return fmt.Errorf("close profile %s session: %w", l.profile.ID, err)
	}
	return nil
}

func (l *profileLane) step(ctx context.Context) error {
	lease := *l.held.Load()
	s := l.supervisor
	request, cancel := boundedContext(ctx, lease.ValidUntil, s.Timing.OperationTimeout)
	owner, err := s.Store.SessionOwnership(request, lease.Deployment, l.profile.ID)
	cancel()
	if err != nil {
		return err
	}
	if owner.Phase == lifecycle.SessionClosed {
		l.session = nil
	}
	if owner.Phase != "" && owner.Phase != lifecycle.SessionClosed &&
		(l.session == nil || owner.Phase != lifecycle.SessionOpen || owner.SessionID != l.session.ID()) {
		return lifecycle.ErrSessionRecoveryRequired
	}
	connection, refreshErr := l.credentials.refresh(ctx, l, lease)
	if refreshErr != nil && l.session != nil && connection != nil {
		s.Logger.Warn("candidate GitHub credential unavailable", "profile", l.profile.ID, "error", refreshErr)
	}
	if connection == nil && refreshErr == nil {
		return errors.New("configured GitHub credential is temporarily unavailable")
	}
	if connection == nil || time.Now().Before(l.credentials.cooldown.deadline()) {
		return refreshErr
	}

	capacity := l.capacity(ctx, lease)
	if time.Now().Before(l.credentials.cooldown.deadline()) {
		return nil
	}
	if l.session == nil || l.version != connection.VersionID {
		if err := l.install(ctx, lease, connection, capacity); err != nil {
			return err
		}
	}
	processor := Processor{
		Store: s.Store, Session: l.session, MaxDemandAge: s.Timing.MaxDemandAge,
		pollTimeout: s.Timing.PollTimeout, operationTimeout: s.Timing.OperationTimeout,
	}
	return processor.step(ctx, lease, l.profile.ID, capacity, func(delivery context.Context, current lifecycle.Lease) *Capacity {
		if time.Now().Before(l.credentials.cooldown.deadline()) {
			return nil
		}
		return l.capacity(delivery, current)
	})
}

func (l *profileLane) install(ctx context.Context, lease lifecycle.Lease, connection *githubsets.Connection, capacity *Capacity) error {
	s := l.supervisor
	request, cancel := boundedContext(ctx, lease.ValidUntil, s.Timing.OperationTimeout)
	defer cancel()
	var session *githubsets.Session
	var err error
	if l.session == nil {
		session, err = l.open(request, lease, connection, capacity)
	} else {
		session, err = connection.ReplaceOwnedSession(request, s.Store, lease, l.session)
	}
	if err != nil {
		return err
	}
	l.session = session
	l.version = connection.VersionID
	return nil
}

const creationEffectID = "create-scale-set"

func (l *profileLane) open(ctx context.Context, lease lifecycle.Lease, connection *githubsets.Connection, capacity *Capacity) (*githubsets.Session, error) {
	store := l.supervisor.Store
	effect, err := store.Effect(ctx, lease.Deployment, l.profile.ID, creationEffectID)
	if err != nil {
		return nil, err
	}
	mayCreate := true
	if effect != nil {
		if err := l.reconcileCreation(ctx, lease, connection, *effect); err != nil {
			return nil, err
		}
		mayCreate = effect.State == lifecycle.EffectPlanned || effect.State == lifecycle.EffectCancelled
	}

	session, err := connection.OpenOwnedSession(ctx, store, lease, l.profile.ID)
	if !errors.Is(err, githubsets.ErrScaleSetMissing) || !mayCreate || capacity == nil {
		return session, err
	}
	if l.profile.State != "active" || l.profile.GitHub.Registration != "create-if-missing" || l.profile.GitHub.ScaleSetID != 0 {
		return nil, err
	}
	if _, err := connection.Create(ctx, store, githubsets.CreateRequest{
		ProfileID: l.profile.ID, EffectID: creationEffectID, Lease: lease,
	}); err != nil {
		return nil, err
	}
	return connection.OpenOwnedSession(ctx, store, lease, l.profile.ID)
}

func (l *profileLane) reconcileCreation(ctx context.Context, lease lifecycle.Lease, connection *githubsets.Connection, effect lifecycle.Effect) error {
	store := l.supervisor.Store
	switch effect.State {
	case lifecycle.EffectConfirmed, lifecycle.EffectCancelled:
		return nil
	case lifecycle.EffectPlanned:
		cancelled, err := store.CancelPlannedEffect(ctx, lease, l.profile.ID, effect.ID, time.Now())
		if err != nil {
			return err
		}
		if !cancelled {
			return lifecycle.ErrRecoveryRequired
		}
		return nil
	case lifecycle.EffectExecuting, lifecycle.EffectUncertain:
		set, err := connection.VerifyCreationReadback(ctx, l.profile.ID, effect)
		if err != nil {
			return errors.Join(lifecycle.ErrRecoveryRequired, err)
		}
		_, err = store.ResolveEffect(ctx, lease, l.profile.ID, effect.ID, time.Now(), true, fmt.Sprintf("scale-set:%d", set.ID))
		return err
	default:
		return lifecycle.ErrRecoveryRequired
	}
}

func (l *profileLane) capacity(ctx context.Context, lease lifecycle.Lease) *Capacity {
	s := l.supervisor
	if l.profile.State == "draining" {
		return &Capacity{}
	}
	if s.Capacity == nil {
		return nil
	}
	request, cancel := boundedContext(ctx, lease.ValidUntil, s.Timing.OperationTimeout)
	defer cancel()
	capacity, err := s.Capacity(request, l.profile)
	if err != nil {
		s.Logger.Warn("runner capacity unavailable", "profile", l.profile.ID)
		return nil
	}
	if capacity == nil || capacity.Maximum < 0 || capacity.OutsideAssigned < 0 {
		return nil
	}
	verified := *capacity
	verified.Maximum = min(verified.Maximum, l.profile.Capacity.MaxRunners)
	return &verified
}
