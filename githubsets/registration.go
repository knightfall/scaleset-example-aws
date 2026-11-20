package githubsets

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/actions/scaleset"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

var (
	ErrScaleSetMissing   = errors.New("configured GitHub scale set was not found")
	ErrRegistrationDrift = errors.New("GitHub runner group or scale set differs from configuration")
	ErrCreateUnconfirmed = errors.New("GitHub scale set creation requires reconciliation")
)

type Connection struct {
	VersionID  string
	client     *scaleset.Client
	deployment string
	scope      string
	profiles   []runtimeconfig.Profile
}

type ScaleSet struct {
	ID      int
	GroupID int
}

type CreateRequest struct {
	ProfileID string
	EffectID  string
	Lease     lifecycle.Lease
}

// Validate checks candidate credentials against every enabled profile bound to
// this connection. It performs lookups without opening any listener sessions.
func (c *Connection) Validate(ctx context.Context) error {
	for _, profile := range c.profiles {
		if _, err := c.lookup(ctx, profile.GitHub); err != nil {
			return fmt.Errorf("verify GitHub profile %s: %w", profile.ID, err)
		}
	}
	return nil
}

func (c *Connection) Lookup(ctx context.Context, profileID string) (ScaleSet, error) {
	profile, err := c.profile(profileID)
	if err != nil {
		return ScaleSet{}, err
	}
	return c.lookup(ctx, profile.GitHub)
}

// Create uses an already held deployment lease and a context whose deadline
// bounds the journaled mutation. The caller serializes effects for the profile.
func (c *Connection) Create(ctx context.Context, journal *lifecycle.Store, request CreateRequest) (ScaleSet, error) {
	profile, err := c.profile(request.ProfileID)
	if err != nil {
		return ScaleSet{}, err
	}
	binding := profile.GitHub
	if profile.State != "active" || binding.Registration != "create-if-missing" || binding.ScaleSetID != 0 {
		return ScaleSet{}, errors.New("profile must explicitly allow creation without a pinned scale set ID")
	}
	deadline, bounded := ctx.Deadline()
	if journal == nil || !bounded || request.Lease.Deployment != c.deployment {
		return ScaleSet{}, errors.New("scale set creation requires the deployment lease, journal and a deadline")
	}
	resolved, err := c.lookup(ctx, binding)
	if !errors.Is(err, ErrScaleSetMissing) {
		return resolved, err
	}
	parameters := scaleset.RunnerScaleSet{
		Name: binding.ScaleSet, RunnerGroupID: resolved.GroupID,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	}
	for _, label := range binding.Labels {
		parameters.Labels = append(parameters.Labels, scaleset.Label{Name: label, Type: "System"})
	}
	payload, err := json.Marshal(struct {
		Scope string
		Set   scaleset.RunnerScaleSet
	}{Scope: c.scope, Set: parameters})
	if err != nil {
		return ScaleSet{}, fmt.Errorf("encode scale set creation intent: %w", err)
	}
	effect := lifecycle.Effect{
		ID: request.EffectID, Kind: "create-scale-set", Target: c.scope,
		RequestSHA256: fmt.Sprintf("%x", sha256.Sum256(payload)), Deadline: deadline,
	}
	if _, err := journal.PlanEffect(ctx, request.Lease, profile.ID, effect, time.Now()); err != nil {
		return ScaleSet{}, err
	}
	if _, err := journal.BeginEffect(ctx, request.Lease, profile.ID, effect.ID, time.Now()); err != nil {
		return ScaleSet{}, err
	}
	return c.createAndConfirm(ctx, journal, request, binding, parameters)
}

func (c *Connection) createAndConfirm(
	ctx context.Context, journal *lifecycle.Store, request CreateRequest, binding runtimeconfig.GitHubBinding, parameters scaleset.RunnerScaleSet,
) (ScaleSet, error) {
	createCtx, status := trackResponseStatus(ctx)
	createCtx = scaleset.WithRequestRetry(createCtx, func(config *scaleset.RetryConfig) {
		config.Max = 0
	})
	created, err := c.client.CreateRunnerScaleSet(createCtx, &parameters)
	if err != nil {
		return ScaleSet{}, fmt.Errorf("%w: %w", ErrCreateUnconfirmed, requestError("create scale set", status.Load(), err))
	}
	confirmed, err := c.lookupSet(ctx, binding, parameters.RunnerGroupID)
	if err != nil {
		return ScaleSet{}, fmt.Errorf("%w: %w", ErrCreateUnconfirmed, err)
	}
	if created == nil || confirmed.ID != created.ID {
		return ScaleSet{}, fmt.Errorf("%w: %w", ErrCreateUnconfirmed, ErrRegistrationDrift)
	}
	evidence := fmt.Sprintf("scale-set:%d", confirmed.ID)
	if _, err := journal.ResolveEffect(ctx, request.Lease, request.ProfileID, request.EffectID, time.Now(), true, evidence); err != nil {
		return ScaleSet{}, fmt.Errorf("%w: %w", ErrCreateUnconfirmed, err)
	}
	return confirmed, nil
}

func (c *Connection) profile(id string) (runtimeconfig.Profile, error) {
	for _, profile := range c.profiles {
		if profile.ID == id {
			return profile, nil
		}
	}
	return runtimeconfig.Profile{}, errors.New("profile is not enabled for this GitHub connection")
}

func (c *Connection) lookup(ctx context.Context, binding runtimeconfig.GitHubBinding) (ScaleSet, error) {
	groupCtx, status := trackResponseStatus(ctx)
	group, err := c.client.GetRunnerGroupByName(groupCtx, binding.RunnerGroup)
	if err != nil {
		return ScaleSet{}, requestError("resolve runner group", status.Load(), err)
	}
	if group == nil || group.ID <= 0 || !strings.EqualFold(group.Name, binding.RunnerGroup) {
		return ScaleSet{}, ErrRegistrationDrift
	}
	return c.lookupSet(ctx, binding, group.ID)
}

func (c *Connection) lookupSet(ctx context.Context, binding runtimeconfig.GitHubBinding, groupID int) (ScaleSet, error) {
	setCtx, status := trackResponseStatus(ctx)
	set, err := c.client.GetRunnerScaleSet(setCtx, groupID, binding.ScaleSet)
	if err != nil {
		return ScaleSet{}, requestError("resolve scale set", status.Load(), err)
	}
	if set == nil {
		return ScaleSet{GroupID: groupID}, ErrScaleSetMissing
	}
	if set.ID <= 0 || set.RunnerGroupID != groupID || !strings.EqualFold(set.Name, binding.ScaleSet) ||
		(binding.ScaleSetID != 0 && int64(set.ID) != binding.ScaleSetID) || !matchingLabels(binding.Labels, set.Labels) {
		return ScaleSet{}, ErrRegistrationDrift
	}
	return ScaleSet{ID: set.ID, GroupID: groupID}, nil
}

func matchingLabels(configured []string, labels []scaleset.Label) bool {
	want := make([]string, 0, len(configured))
	for _, value := range configured {
		want = append(want, strings.ToLower(value))
	}
	got := make([]string, 0, len(labels))
	for _, label := range labels {
		got = append(got, strings.ToLower(label.Name))
	}
	slices.Sort(want)
	slices.Sort(got)
	return slices.Equal(want, got)
}
