package planner_test

import (
	"slices"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/planner"
)

var now = time.Date(2026, time.November, 21, 0, 3, 0, 0, time.UTC)

func TestPlansWithinHardLimitWithExpiredAndUnknownCapacity(t *testing.T) {
	limits := planner.Limits{MaxRunners: 10, IdleReserve: 2, StartupTimeout: 5 * time.Minute}
	demand := &planner.Demand{ObservedAt: now, Assigned: 6, Running: 1}
	inventory := &planner.Inventory{
		ObservedAt: now, Complete: true, BackendLimit: new(8),
		Executors: []planner.Executor{
			{ID: "busy", State: "busy", Owned: true},
			{ID: "idle", State: "idle", Owned: true},
			{ID: "boot", State: "starting", Owned: true, StartedAt: now.Add(-time.Minute)},
			{ID: "expired", State: "starting", Owned: true, StartedAt: now.Add(-6 * time.Minute)},
			{ID: "foreign", State: "unknown"},
			{ID: "spent", State: "completed", Owned: true},
			{ID: "unoffered", State: "unused", Owned: true},
		},
	}
	result, err := planner.Calculate(limits, demand, inventory, now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if result.EffectiveMax != 8 || *result.Target != 8 || *result.Deficit != 5 ||
		*result.Serving != 2 || *result.CrediblePending != 1 || *result.Unknown != 2 ||
		*result.Occupied != 7 || *result.Headroom != 1 || result.ProposedStarts != 1 {
		t.Fatalf("unexpected capacity: %+v", result)
	}
	if !slices.Equal(result.CleanupCandidates, []string{"spent", "unoffered"}) ||
		!slices.Equal(result.OrphanCandidates, []string{"foreign"}) {
		t.Fatalf("unexpected cleanup or orphan candidates: %+v", result)
	}
}

func TestMissingStaleOrPartialEvidenceDoesNotProduceStartProposals(t *testing.T) {
	limits := planner.Limits{MaxRunners: 10, IdleReserve: 2, StartupTimeout: time.Minute}
	recent := &planner.Demand{ObservedAt: now, Assigned: 4}
	stale := &planner.Demand{ObservedAt: now.Add(-time.Hour), Assigned: 4}
	freshInventory := &planner.Inventory{ObservedAt: now, Complete: true}
	for _, tc := range []struct {
		name, demandStatus, inventoryStatus string
		demand                              *planner.Demand
		inventory                           *planner.Inventory
		targetKnown                         bool
	}{
		{"no evidence", "unknown", "unknown", nil, nil, false},
		{"inventory only", "unknown", "fresh", nil, freshInventory, false},
		{"stale demand", "stale", "fresh", stale, freshInventory, false},
		{"no inventory", "fresh", "unknown", recent, nil, true},
		{"partial inventory", "fresh", "partial", recent, &planner.Inventory{ObservedAt: now}, true},
		{"stale inventory", "fresh", "stale", recent, &planner.Inventory{ObservedAt: now.Add(-time.Hour), Complete: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := planner.Calculate(limits, tc.demand, tc.inventory, now, 10*time.Minute)
			if err != nil || result.DemandStatus != tc.demandStatus || result.InventoryStatus != tc.inventoryStatus ||
				(result.Target != nil) != tc.targetKnown || result.Deficit != nil || result.ProposedStarts != 0 {
				t.Fatalf("unexpected capacity: %+v, error %v", result, err)
			}
		})
	}
}

func TestDrainingRemovesWarmReserveAndKeepsBusyCapacity(t *testing.T) {
	limits := planner.Limits{MaxRunners: 3, IdleReserve: 3, Draining: true, StartupTimeout: time.Minute}
	inventory := &planner.Inventory{
		ObservedAt: now, Complete: true, Executors: []planner.Executor{{ID: "busy", State: "busy", Owned: true}},
	}
	result, err := planner.Calculate(limits, &planner.Demand{ObservedAt: now, Assigned: 2, Running: 1}, inventory, now, time.Minute)
	if err != nil || result.EffectiveReserve != 0 || *result.Target != 2 || result.ProposedStarts != 1 ||
		len(result.CleanupCandidates) != 0 {
		t.Fatalf("unexpected drain result: %+v, error %v", result, err)
	}
	result, err = planner.Calculate(limits, &planner.Demand{ObservedAt: now}, inventory, now, time.Minute)
	if err != nil || *result.Target != 0 || result.ProposedStarts != 0 || len(result.CleanupCandidates) != 0 {
		t.Fatalf("empty drain can affect busy capacity: %+v, error %v", result, err)
	}
}

func TestUnmatchedRunningJobsBlockStartProposals(t *testing.T) {
	limits := planner.Limits{MaxRunners: 5, StartupTimeout: time.Minute}
	demand := &planner.Demand{ObservedAt: now, Assigned: 4, Running: 2}
	inventory := &planner.Inventory{ObservedAt: now, Complete: true}
	result, err := planner.Calculate(limits, demand, inventory, now, time.Minute)
	if err != nil || *result.UnmappedRunning != 2 || *result.Deficit != 2 || result.ProposedStarts != 0 ||
		!slices.Contains(result.Blockers, "running jobs lack matched executors") {
		t.Fatalf("unmatched jobs did not block starts: %+v, error %v", result, err)
	}
}

func TestRejectsDuplicateExecutorsAndContradictoryDemand(t *testing.T) {
	limits := planner.Limits{MaxRunners: 5, StartupTimeout: time.Minute}
	if _, err := planner.Calculate(limits, &planner.Demand{ObservedAt: now, Running: 1}, nil, now, time.Minute); err == nil {
		t.Fatal("accepted more running jobs than assigned jobs")
	}
	inventory := &planner.Inventory{ObservedAt: now, Executors: []planner.Executor{
		{ID: "same", State: "idle", Owned: true}, {ID: "same", State: "starting", Owned: true},
	}}
	if _, err := planner.Calculate(limits, nil, inventory, now, time.Minute); err == nil {
		t.Fatal("accepted duplicate executor observations")
	}
}
