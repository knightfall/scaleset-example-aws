package listener

import (
	"testing"

	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

func TestProfileBindingNormalizesSelectorsAndPinsExecutionIdentity(t *testing.T) {
	var artifact runtimeconfig.Artifact
	artifact.Config.Spec.GitHub = map[string]runtimeconfig.GitHubConnection{
		"build": {
			ScopeURL: "https://github.com/example", Auth: runtimeconfig.GitHubAuth{Type: "github-app"},
		},
	}
	profile := runtimeconfig.Profile{
		ID: "build", State: "active", Purpose: "docker",
		Platform: runtimeconfig.Platform{OS: "linux", Arch: "x64"},
		Capacity: runtimeconfig.Capacity{MaxRunners: 10},
		GitHub: runtimeconfig.GitHubBinding{
			Connection: "build", RunnerGroup: "Build Runners", ScaleSet: "LINUX",
			ScaleSetID: 7, Labels: []string{"LINUX", "DOCKER"},
		},
		Provisioner: runtimeconfig.Provisioner{Type: "asg", GroupARN: "arn:aws:autoscaling:region:account:autoScalingGroup:group1"},
	}
	artifact.Config.Spec.Profiles = []runtimeconfig.Profile{profile}
	first, err := configurationRollout(artifact, 1)
	if err != nil {
		t.Fatal(err)
	}

	profile.GitHub.RunnerGroup = "build runners"
	profile.GitHub.ScaleSet = "linux"
	profile.GitHub.Labels = []string{"docker", "linux"}
	profile.Capacity.MaxRunners = 20
	artifact.Config.Spec.Profiles = []runtimeconfig.Profile{profile}
	next, err := configurationRollout(artifact, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest == next.Digest || first.Profiles[0].BindingDigest != next.Profiles[0].BindingDigest {
		t.Fatal("limits or selector normalization unexpectedly changed the binding or reused the configuration digest")
	}

	profile.Provisioner.GroupARN = "arn:aws:autoscaling:region:account:autoScalingGroup:group2"
	artifact.Config.Spec.Profiles = []runtimeconfig.Profile{profile}
	moved, err := configurationRollout(artifact, 3)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Profiles[0].BindingDigest == next.Profiles[0].BindingDigest {
		t.Fatal("a different ASG reused the binding digest")
	}
}
