package listener

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type Timing struct {
	LeaseTTL           time.Duration
	OperationTimeout   time.Duration
	PollTimeout        time.Duration
	CredentialInterval time.Duration
	MaxDemandAge       time.Duration
	ShutdownTimeout    time.Duration
}

func DefaultTiming() Timing {
	return Timing{
		LeaseTTL: 5 * time.Minute, OperationTimeout: 30 * time.Second, PollTimeout: 150 * time.Second,
		CredentialInterval: 5 * time.Minute, MaxDemandAge: 10 * time.Minute, ShutdownTimeout: 30 * time.Second,
	}
}

type Supervisor struct {
	Store                   *lifecycle.Store
	Loader                  githubsets.Loader
	Timing                  Timing
	ConfigurationGeneration int64
	// Capacity returns verified backend headroom. Nil keeps admission closed.
	Capacity func(context.Context, runtimeconfig.Profile) (*Capacity, error)
	Logger   *slog.Logger
}

// Run supervises an already acquired and authorized deployment lease. Shutdown
// joins the polling lanes before releasing ownership.
func (s Supervisor) Run(ctx context.Context, artifact runtimeconfig.Artifact, lease lifecycle.Lease) error {
	if artifact.Config.Spec.Mode != "active" || lease.Deployment != artifact.Config.Metadata.Name ||
		s.Store == nil || s.Loader.Secrets == nil || s.ConfigurationGeneration <= 0 {
		return errors.New("supervision requires active configuration, a generation, matching lease and active clients")
	}
	if s.Timing == (Timing{}) {
		s.Timing = DefaultTiming()
	}
	timing := s.Timing
	if timing.LeaseTTL/3 <= 0 || timing.OperationTimeout <= 0 || timing.PollTimeout <= 0 ||
		timing.CredentialInterval <= 0 || timing.MaxDemandAge <= 0 || timing.ShutdownTimeout <= 0 {
		return errors.New("supervision deadlines and intervals must be positive")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if lease.Phase != lifecycle.LeaseActive {
		return lifecycle.ErrRecoveryRequired
	}
	current, err := s.Store.Lease(ctx, lease.Deployment)
	if err != nil {
		return err
	}
	if current == nil || current.Owner != lease.Owner || current.Epoch != lease.Epoch ||
		current.Phase != lifecycle.LeaseActive || !current.ValidUntil.After(time.Now()) {
		return lifecycle.ErrLeaseLost
	}

	rollout, err := configurationRollout(artifact, s.ConfigurationGeneration)
	if err != nil {
		return err
	}
	if _, err := s.Store.ApplyRollout(ctx, *current, rollout, time.Now()); err != nil {
		return fmt.Errorf("activate configuration: %w", err)
	}

	polling, cancelPolling := context.WithCancel(ctx)
	defer cancelPolling()
	heartbeat, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeat()
	var held atomic.Pointer[lifecycle.Lease]
	held.Store(current)
	lanes := s.profileLanes(artifact, &held)
	if len(lanes) == 0 {
		return errors.New("supervision requires at least one enabled profile")
	}
	heartbeatResult := make(chan error, 1)
	go func() {
		err := s.renewLease(heartbeat, &held)
		if err != nil {
			cancelPolling()
		}
		heartbeatResult <- err
	}()

	laneResults := make(chan error, len(lanes))
	for _, lane := range lanes {
		go func() {
			laneResults <- lane.run(polling)
		}()
	}
	var shutdownErr error
	for range lanes {
		shutdownErr = errors.Join(shutdownErr, <-laneResults)
	}
	stopHeartbeat()
	if err := errors.Join(shutdownErr, <-heartbeatResult); err != nil {
		return err
	}
	lease = *held.Load()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), timing.ShutdownTimeout)
	defer cancel()
	return s.releaseWhenSettled(cleanup, artifact.Config.Spec.Profiles, lease)
}

func (s Supervisor) renewLease(ctx context.Context, held *atomic.Pointer[lifecycle.Lease]) error {
	interval := s.Timing.LeaseTTL / 3
	for {
		lease := held.Load()
		if !waitUntil(ctx, lease.ValidUntil.Add(-2*interval)) {
			return nil
		}
		request, cancel := boundedContext(ctx, lease.ValidUntil, s.Timing.OperationTimeout)
		next, err := s.Store.RenewLease(request, *lease, time.Now(), s.Timing.LeaseTTL)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("renew deployment lease: %w", err)
		}
		held.Store(&next)
	}
}

func (s Supervisor) releaseWhenSettled(ctx context.Context, profiles []runtimeconfig.Profile, lease lifecycle.Lease) error {
	for _, profile := range profiles {
		owner, err := s.Store.SessionOwnership(ctx, lease.Deployment, profile.ID)
		if err != nil {
			return err
		}
		if owner.Phase != "" && owner.Phase != lifecycle.SessionClosed {
			return fmt.Errorf("profile %s: %w", profile.ID, lifecycle.ErrSessionRecoveryRequired)
		}
		if _, err := s.Store.CancelPlannedEffect(ctx, lease, profile.ID, creationEffectID, time.Now()); err != nil {
			return err
		}
		settled, err := s.Store.ProfileEffectsSettled(ctx, lease.Deployment, profile.ID)
		if err != nil {
			return err
		}
		if !settled {
			return fmt.Errorf("profile %s: %w", profile.ID, lifecycle.ErrRecoveryRequired)
		}
	}
	return s.Store.ReleaseLease(ctx, lease, time.Now(), lifecycle.Quiescence{SessionsClosed: true, EffectsSettled: true})
}

func (s Supervisor) profileLanes(artifact runtimeconfig.Artifact, lease *atomic.Pointer[lifecycle.Lease]) []profileLane {
	connections := make(map[string]*credentialState)
	limits := &credentialLimits{byKey: make(map[[32]byte]*credentialCooldown)}
	var lanes []profileLane
	for _, profile := range artifact.Config.Spec.Profiles {
		if profile.State == "disabled" {
			continue
		}
		name := profile.GitHub.Connection
		if connections[name] == nil {
			state := &credentialState{limits: limits}
			secretARN := artifact.Config.Spec.GitHub[name].Auth.SecretARN
			key := sha256.Sum256([]byte("secrets-manager:" + secretARN))
			state.cooldown.Store(limits.forCredential(key))
			connections[name] = state
		}
		lanes = append(lanes, profileLane{
			supervisor: s, artifact: artifact, profile: profile, held: lease, credentials: connections[name],
		})
	}
	return lanes
}

func boundedContext(ctx context.Context, expiry time.Time, timeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(timeout)
	if expiry.Before(deadline) {
		deadline = expiry
	}
	return context.WithDeadline(ctx, deadline)
}

func waitUntil(ctx context.Context, deadline time.Time) bool {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

type credentialLimits struct {
	mu    sync.Mutex
	byKey map[[32]byte]*credentialCooldown
}

func (l *credentialLimits) forCredential(key [32]byte) *credentialCooldown {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byKey[key] == nil {
		l.byKey[key] = &credentialCooldown{}
	}
	return l.byKey[key]
}

type credentialCooldown struct {
	mu    sync.Mutex
	until time.Time
}

func (c *credentialCooldown) deadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.until
}

func (c *credentialCooldown) record(rateLimit *githubsets.RateLimitError) {
	now := time.Now()
	until := now.Add(rateLimit.RetryAfter)
	if rateLimit.RetryAfter <= 0 && !rateLimit.Reset.After(now) {
		until = now.Add(time.Minute)
	}
	if rateLimit.Reset.After(until) {
		until = rateLimit.Reset
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if until.After(c.until) {
		c.until = until
	}
}

func retryDelay(failures int) time.Duration {
	delay := min(30*time.Second, 250*time.Millisecond<<min(failures, 7))
	return delay/2 + rand.N(delay/2)
}
