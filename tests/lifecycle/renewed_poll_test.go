package lifecycle_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/listener"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

func TestSupervisorCompletesPollAcrossLeaseRenewal(t *testing.T) {
	ledger, lease, _, remote, supervisor, _ := supervisedFixture(t, false)
	artifact := githubCreationConfig()
	artifact.Config.Spec.Profiles[0].Capacity.MaxRunners = 3
	supervisor.Timing.PollTimeout = 4 * time.Second
	var proven atomic.Bool
	proven.Store(true)
	supervisor.Capacity = func(context.Context, runtimeconfig.Profile) (*listener.Capacity, error) {
		if proven.Load() {
			return &listener.Capacity{Maximum: 3}, nil
		}
		return nil, nil
	}
	remote.invalidateCapacity = func() {
		// Return this first delivery after the deadline of the original lease.
		time.Sleep(time.Until(lease.ValidUntil.Add(25 * time.Millisecond)))
		proven.Store(false)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, artifact, lease) }()
	waitForSupervisorEvents(t, remote.events, "acked:7")

	// The acknowledgement response must also be durable before shutdown.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		progress, err := ledger.SessionProgress(ctx, "dev", "build")
		if err != nil {
			t.Fatal(err)
		}
		if progress.LastAcknowledged == 1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("acknowledgement did not become durable")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
}
