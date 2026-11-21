package listener

import (
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/githubsets"
)

func TestNamedConnectionsShareCooldownOnlyForTheSameCredential(t *testing.T) {
	limits := &credentialLimits{byKey: make(map[[32]byte]*credentialCooldown)}
	primary, alias, independent := &credentialState{}, &credentialState{}, &credentialState{}
	primary.cooldown.Store(limits.forCredential([32]byte{1}))
	alias.cooldown.Store(limits.forCredential([32]byte{1}))
	independent.cooldown.Store(limits.forCredential([32]byte{2}))

	primary.recordLimit(&githubsets.RateLimitError{RetryAfter: time.Minute})
	if !alias.cooldownDeadline().After(time.Now()) || !alias.cooldownDeadline().Equal(primary.cooldownDeadline()) {
		t.Fatal("connection alias did not observe the credential cooldown")
	}
	if !independent.cooldownDeadline().IsZero() {
		t.Fatal("rate limit delayed an independent credential")
	}
}
