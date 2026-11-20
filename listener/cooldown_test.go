package listener

import (
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

func TestCredentialCooldownIsSharedAcrossAliasesOfSameSecret(t *testing.T) {
	var artifact runtimeconfig.Artifact
	artifact.Config.Spec.GitHub = map[string]runtimeconfig.GitHubConnection{
		"one":   {Auth: runtimeconfig.GitHubAuth{SecretARN: "shared"}},
		"two":   {Auth: runtimeconfig.GitHubAuth{SecretARN: "shared"}},
		"other": {Auth: runtimeconfig.GitHubAuth{SecretARN: "different"}},
	}
	for _, connection := range []string{"one", "two", "other"} {
		artifact.Config.Spec.Profiles = append(artifact.Config.Spec.Profiles, runtimeconfig.Profile{
			ID: connection, State: "active", GitHub: runtimeconfig.GitHubBinding{Connection: connection},
		})
	}
	lanes := (Supervisor{}).profileLanes(artifact, nil)
	reset := time.Now().Add(5 * time.Second)
	lanes[0].credentials.cooldown.record(&githubsets.RateLimitError{RetryAfter: time.Second, Reset: reset})
	lanes[1].credentials.cooldown.record(&githubsets.RateLimitError{RetryAfter: 2 * time.Second})
	if got := lanes[1].credentials.cooldown.deadline(); !got.Equal(reset) {
		t.Fatalf("shared cooldown = %v, want %v", got, reset)
	}
	if got := lanes[2].credentials.cooldown.deadline(); !got.IsZero() {
		t.Fatalf("unrelated credential cooldown = %v", got)
	}
}
