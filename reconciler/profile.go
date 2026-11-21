package reconciler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/planner"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type ObservedResource struct {
	ResourceID string
	// An empty incarnation keeps ownership unverified.
	Incarnation string
	Observation lifecycle.ResourceObservation
}

type Snapshot struct {
	ObservedAt   time.Time
	Reference    string
	Complete     bool
	BackendLimit *int
	Resources    []ObservedResource
}

// InventoryProvider reports the configured runner pool with canonical AWS identities.
// Missing resources remain unknown until an exact terminal observation proves
// absence. A failed lookup returns an error, rather than an empty snapshot.
type InventoryProvider interface {
	Inspect(context.Context, runtimeconfig.Profile) (Snapshot, error)
}

type ProfilePlan struct {
	planner.Result
	CapacityVerified bool `json:"capacityVerified"`
}

type Coordinator struct {
	Store               *lifecycle.Store
	Provider            InventoryProvider
	Generation          int64
	ConfigurationDigest string
	MaxObservationAge   time.Duration
	ExitGrace           time.Duration
	Now                 func() time.Time
}

// Reconcile records observations and returns a plan. Call it on the profile's
// serialized effect lane. Provider mutations and new allocations must use that
// same lane, and dispatch must recheck ownership and exact resource evidence.
func (c Coordinator) Reconcile(ctx context.Context, lease lifecycle.Lease, profile runtimeconfig.Profile) (ProfilePlan, error) {
	now := c.now()
	if lease.Phase != lifecycle.LeaseActive && lease.Phase != lifecycle.LeaseRecovering {
		return ProfilePlan{}, lifecycle.ErrLeaseLost
	}
	limits := planner.Limits{
		MaxRunners: profile.Capacity.MaxRunners, IdleReserve: profile.Capacity.IdleReserve,
		Draining:       profile.State == lifecycle.ProfileDraining,
		StartupTimeout: time.Duration(profile.Capacity.StartupTimeoutSeconds) * time.Second,
	}
	if c.Store == nil || c.Provider == nil || c.Generation <= 0 || c.ConfigurationDigest == "" || profile.ID == "" || c.ExitGrace <= 0 {
		return ProfilePlan{}, errors.New("profile reconciliation requires configured dependencies and identity")
	}
	if profile.State != lifecycle.ProfileActive && profile.State != lifecycle.ProfileDraining {
		return ProfilePlan{}, errors.New("profile reconciliation requires an active or draining profile")
	}
	if _, err := planner.Calculate(limits, nil, nil, now, c.MaxObservationAge); err != nil {
		return ProfilePlan{}, err
	}
	ctx, cancel := context.WithDeadline(ctx, lease.ValidUntil)
	defer cancel()
	if err := c.verifyOwner(ctx, lease, profile); err != nil {
		return ProfilePlan{}, err
	}
	snapshot, err := c.Provider.Inspect(ctx, profile)
	if err != nil {
		return ProfilePlan{}, fmt.Errorf("inspect profile resources: %w", err)
	}
	resources, err := snapshot.resources(c.now())
	if err != nil {
		return ProfilePlan{}, err
	}
	inventory, err := c.inventory(ctx, lease, profile.ID, snapshot, resources)
	if err != nil {
		return ProfilePlan{}, err
	}
	demand, err := c.demand(ctx, lease, profile.ID)
	if err != nil {
		return ProfilePlan{}, err
	}
	calculation, err := planner.Calculate(limits, demand, &inventory, c.now(), c.MaxObservationAge)
	result := ProfilePlan{Result: calculation}
	if err != nil {
		return result, err
	}
	settled, err := c.Store.ProfileEffectsSettled(ctx, lease.Deployment, profile.ID)
	if err != nil {
		return ProfilePlan{}, err
	}
	result.CapacityVerified = result.InventoryStatus == "fresh" && snapshot.BackendLimit != nil && settled && lease.Phase == lifecycle.LeaseActive
	if !result.CapacityVerified {
		result.ProposedStarts = 0
	}
	if snapshot.BackendLimit == nil {
		result.Blockers = append(result.Blockers, "backend limit unavailable")
	}
	if !settled || lease.Phase != lifecycle.LeaseActive {
		result.CleanupCandidates = []string{}
		if !settled {
			result.Blockers = append(result.Blockers, "unfinished lifecycle effects")
		}
		if lease.Phase != lifecycle.LeaseActive {
			result.Blockers = append(result.Blockers, "deployment ownership recovering")
		}
	}
	if err := c.verifyOwner(ctx, lease, profile); err != nil {
		return ProfilePlan{}, err
	}
	return result, nil
}

func (s Snapshot) resources(now time.Time) (map[string]ObservedResource, error) {
	if s.ObservedAt.IsZero() || s.ObservedAt.After(now) || s.Reference == "" || (s.BackendLimit != nil && *s.BackendLimit < 0) {
		return nil, errors.New("invalid resource inventory snapshot")
	}
	resources := make(map[string]ObservedResource, len(s.Resources))
	for _, resource := range s.Resources {
		id := resource.ResourceID
		if id == "" || strings.TrimSpace(id) != id || strings.TrimSpace(resource.Incarnation) != resource.Incarnation {
			return nil, errors.New("inventory requires canonical resource identities")
		}
		if _, duplicate := resources[id]; duplicate {
			return nil, errors.New("inventory repeats a resource identity")
		}
		if resource.Observation.ObservedAt.After(s.ObservedAt) {
			return nil, errors.New("resource observation is newer than its inventory snapshot")
		}
		resources[id] = resource
	}
	return resources, nil
}

func (c Coordinator) verifyOwner(ctx context.Context, lease lifecycle.Lease, profile runtimeconfig.Profile) error {
	current, err := c.Store.Lease(ctx, lease.Deployment)
	if err != nil {
		return err
	}
	now := c.now()
	if current == nil || current.Owner != lease.Owner || current.Epoch != lease.Epoch || current.Phase != lease.Phase ||
		!current.ValidUntil.After(now) || !lease.ValidUntil.After(now) {
		return lifecycle.ErrLeaseLost
	}
	catalog, err := c.Store.Catalog(ctx, lease.Deployment)
	if err != nil {
		return err
	}
	entry, found := catalog.Profiles[profile.ID]
	if catalog.Generation != c.Generation || catalog.Digest != c.ConfigurationDigest || catalog.Epoch > lease.Epoch ||
		(lease.Phase == lifecycle.LeaseActive && catalog.Epoch != lease.Epoch) || !found || entry.State != profile.State {
		return lifecycle.ErrConfigurationGeneration
	}
	return ctx.Err()
}

func (c Coordinator) demand(ctx context.Context, lease lifecycle.Lease, profile string) (*planner.Demand, error) {
	owner, err := c.Store.SessionOwnership(ctx, lease.Deployment, profile)
	if err != nil {
		return nil, err
	}
	progress, err := c.Store.SessionProgress(ctx, lease.Deployment, profile)
	if err != nil {
		return nil, err
	}
	if owner.Phase != lifecycle.SessionOpen || owner.Epoch != lease.Epoch || owner.ControllerID != lease.Owner ||
		owner.SessionID != progress.SessionID || progress.Demand == nil {
		return nil, nil
	}
	return &planner.Demand{ObservedAt: progress.Demand.ObservedAt, Assigned: progress.Demand.Assigned, Running: progress.Demand.Running}, nil
}

func (c Coordinator) inventory(ctx context.Context, lease lifecycle.Lease, profile string, snapshot Snapshot, resources map[string]ObservedResource) (planner.Inventory, error) {
	inventory := planner.Inventory{ObservedAt: snapshot.ObservedAt, Complete: snapshot.Complete, BackendLimit: snapshot.BackendLimit}
	seen := make(map[string]bool)
	var cursor lifecycle.Key
	for {
		executors, next, err := c.Store.Executors(ctx, lease.Deployment, profile, cursor, 100)
		if err != nil {
			return planner.Inventory{}, err
		}
		for _, executor := range executors {
			id := executor.Binding.ResourceID
			if seen[id] {
				return planner.Inventory{}, errors.New("durable allocations repeat a resource identity")
			}
			seen[id] = true
			resource, present := resources[id]
			delete(resources, id)
			entry, err := c.observe(ctx, lease, profile, executor, resource, present, snapshot)
			if err != nil {
				return planner.Inventory{}, err
			}
			inventory.Executors = append(inventory.Executors, entry)
		}
		if next == (lifecycle.Key{}) {
			break
		}
		if cursor != (lifecycle.Key{}) && next.SK <= cursor.SK {
			return planner.Inventory{}, errors.New("executor query did not advance its cursor")
		}
		cursor = next
	}
	for id := range resources {
		inventory.Executors = append(inventory.Executors, planner.Executor{ID: id, State: "unknown"})
	}
	return inventory, nil
}

func (c Coordinator) observe(ctx context.Context, lease lifecycle.Lease, profile string, executor lifecycle.Executor, resource ObservedResource, present bool, snapshot Snapshot) (planner.Executor, error) {
	if present && resource.Incarnation != executor.Binding.Incarnation {
		if resource.Incarnation != "" && c.now().Sub(snapshot.ObservedAt) <= c.MaxObservationAge {
			if err := c.Store.MarkExecutorConflict(ctx, lease, profile, executor.Binding, snapshot.Reference, c.now()); err != nil {
				return planner.Executor{}, err
			}
		}
		return planner.Executor{ID: executor.Binding.ResourceID, State: "unknown"}, nil
	}
	observation := resource.Observation
	if !present {
		observation = lifecycle.ResourceObservation{State: lifecycle.ResourceUnknown, ObservedAt: snapshot.ObservedAt, Reference: snapshot.Reference}
	}
	if err := c.Store.ObserveResource(ctx, lease, profile, executor.Binding, observation, c.now()); err != nil {
		return planner.Executor{}, err
	}
	current, err := c.Store.Executor(ctx, lease.Deployment, profile, executor.Binding.AllocationID)
	if err != nil {
		return planner.Executor{}, err
	}
	if current == nil || current.Binding != executor.Binding {
		return planner.Executor{}, lifecycle.ErrExecutorIdentity
	}
	if !present && current.Resource.State != lifecycle.ResourceTerminal {
		return planner.Executor{ID: executor.Binding.ResourceID, Owned: true, State: "unknown"}, nil
	}
	return ClassifyExecutor(*current, c.now(), c.MaxObservationAge, c.ExitGrace)
}

func (c Coordinator) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}
