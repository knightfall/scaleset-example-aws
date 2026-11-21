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

func TestInFlightRateLimitStillAppliesToCredentialBeforeRotation(t *testing.T) {
	limits := &credentialLimits{byKey: make(map[[32]byte]*credentialCooldown)}
	oldKey, newKey := [32]byte{1}, [32]byte{2}
	connection, oldAlias := &credentialState{limits: limits}, &credentialState{limits: limits}
	connection.cooldown.Store(limits.forCredential(oldKey))
	oldAlias.cooldown.Store(limits.forCredential(oldKey))

	// Rotate while a poll still uses the old session, then receive that poll's response.
	connection.cooldown.Store(limits.forCredential(newKey))
	connection.recordLimit(&githubsets.RateLimitError{RetryAfter: time.Minute, CredentialKey: oldKey})
	if !oldAlias.cooldownDeadline().After(time.Now()) {
		t.Fatal("an alias of the throttled credential was not delayed")
	}
	if !connection.cooldownDeadline().IsZero() {
		t.Fatal("the in-flight response delayed the replacement credential")
	}
}
