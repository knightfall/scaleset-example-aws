package listener

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

func configurationRollout(artifact runtimeconfig.Artifact, generation int64) (lifecycle.Rollout, error) {
	payload, err := json.Marshal(artifact)
	if err != nil {
		return lifecycle.Rollout{}, fmt.Errorf("encode validated runtime configuration: %w", err)
	}
	rollout := lifecycle.Rollout{Generation: generation, Digest: fmt.Sprintf("%x", sha256.Sum256(payload))}
	for _, profile := range artifact.Config.Spec.Profiles {
		desired := lifecycle.DesiredProfile{ID: profile.ID, State: profile.State}
		if profile.State != lifecycle.ProfileDisabled {
			desired.BindingDigest, err = profileBindingDigest(artifact, profile)
			if err != nil {
				return lifecycle.Rollout{}, fmt.Errorf("profile %s binding: %w", profile.ID, err)
			}
		}
		rollout.Profiles = append(rollout.Profiles, desired)
	}
	return rollout, nil
}

func profileBindingDigest(artifact runtimeconfig.Artifact, profile runtimeconfig.Profile) (string, error) {
	connection, present := artifact.Config.Spec.GitHub[profile.GitHub.Connection]
	if !present {
		return "", errors.New("missing GitHub connection")
	}
	scope, err := connection.CanonicalScope()
	if err != nil {
		return "", err
	}
	labels := make([]string, 0, len(profile.GitHub.Labels))
	for _, label := range profile.GitHub.Labels {
		labels = append(labels, strings.ToLower(label))
	}
	slices.Sort(labels)

	provider := profile.Provisioner
	var network runtimeconfig.Network
	if provider.Type == "fargate" {
		var ok bool
		network, ok = artifact.Config.Spec.Networks[provider.Network]
		if !ok {
			return "", errors.New("missing Fargate network")
		}
		provider.Cluster = artifact.Resources.ECSClusterARNs[provider.Cluster]
		if provider.Cluster == "" {
			return "", errors.New("missing resolved Fargate cluster")
		}
		provider.Network = ""
		network.Subnets = slices.Clone(network.Subnets)
		slices.SortFunc(network.Subnets, func(a, b runtimeconfig.Subnet) int {
			return cmp.Compare(a.ID, b.ID)
		})
		network.SecurityGroupIDs = slices.Clone(network.SecurityGroupIDs)
		slices.Sort(network.SecurityGroupIDs)
	}
	binding := struct {
		Scope       string
		Group       string
		ScaleSet    string
		ScaleSetID  int64
		Labels      []string
		Platform    runtimeconfig.Platform
		Purpose     string
		Provisioner runtimeconfig.Provisioner
		Network     runtimeconfig.Network
	}{
		Scope: scope, Group: strings.ToLower(profile.GitHub.RunnerGroup),
		ScaleSet: strings.ToLower(profile.GitHub.ScaleSet), ScaleSetID: profile.GitHub.ScaleSetID, Labels: labels,
		Platform: profile.Platform, Purpose: profile.Purpose, Provisioner: provider, Network: network,
	}
	payload, err := json.Marshal(binding)
	if err != nil {
		return "", fmt.Errorf("encode immutable profile binding: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}
