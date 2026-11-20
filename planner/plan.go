// Package planner computes proposed runner capacity without external effects.
package planner

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

type Limits struct {
	MaxRunners     int
	IdleReserve    int
	Draining       bool
	StartupTimeout time.Duration
}

type Demand struct {
	ObservedAt time.Time
	Assigned   int
	Running    int
}

type Executor struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	Owned     bool      `json:"owned"`
	StartedAt time.Time `json:"startedAt,omitempty"`
}

type Inventory struct {
	ObservedAt   time.Time
	Complete     bool
	BackendLimit *int
	Executors    []Executor
}

type Result struct {
	DemandStatus      string   `json:"demandStatus"`
	InventoryStatus   string   `json:"inventoryStatus"`
	EffectiveMax      int      `json:"effectiveMax"`
	EffectiveReserve  int      `json:"effectiveReserve"`
	Assigned          *int     `json:"assignedJobs"`
	Running           *int     `json:"runningJobs"`
	Serving           *int     `json:"servingRunners"`
	Busy              *int     `json:"busyRunners"`
	UnmappedRunning   *int     `json:"unmappedRunningJobs"`
	CrediblePending   *int     `json:"crediblePending"`
	Occupied          *int     `json:"occupiedSlots"`
	Unknown           *int     `json:"unknownSlots"`
	Headroom          *int     `json:"headroom"`
	Target            *int     `json:"target"`
	Deficit           *int     `json:"deficit"`
	ProposedStarts    int      `json:"proposedStarts"`
	CleanupCandidates []string `json:"cleanupCandidates"`
	OrphanCandidates  []string `json:"orphanCandidates"`
	Blockers          []string `json:"blockers"`
}

// Calculate plans from observations at now. A nil observation denotes unknown data.
func Calculate(limits Limits, demand *Demand, inventory *Inventory, now time.Time, maxAge time.Duration) (Result, error) {
	result := Result{
		DemandStatus: "unknown", InventoryStatus: "unknown",
		EffectiveMax: limits.MaxRunners, EffectiveReserve: limits.IdleReserve,
		CleanupCandidates: []string{}, OrphanCandidates: []string{}, Blockers: []string{},
	}
	if limits.MaxRunners <= 0 || limits.IdleReserve < 0 || limits.IdleReserve > limits.MaxRunners ||
		limits.StartupTimeout <= 0 || maxAge <= 0 || now.IsZero() {
		return result, errors.New("invalid capacity limits, observation age, or planning time")
	}
	if limits.Draining {
		result.EffectiveReserve = 0
	}
	if demand != nil {
		if demand.ObservedAt.IsZero() || demand.Assigned < 0 || demand.Running < 0 || demand.Running > demand.Assigned {
			return result, errors.New("invalid demand observation")
		}
		result.DemandStatus = "stale"
		assigned, running := demand.Assigned, demand.Running
		result.Assigned = &assigned
		result.Running = &running
		if fresh(demand.ObservedAt, now, maxAge) {
			result.DemandStatus = "fresh"
		}
	}
	if inventory != nil {
		if err := result.observe(limits, *inventory, now, maxAge); err != nil {
			return result, err
		}
	}
	result.EffectiveReserve = min(result.EffectiveReserve, result.EffectiveMax)
	demandFresh := demand != nil && result.DemandStatus == "fresh"
	inventoryFresh := result.InventoryStatus == "fresh"
	if !demandFresh {
		result.Blockers = append(result.Blockers, "demand "+result.DemandStatus)
	}
	if !inventoryFresh {
		result.Blockers = append(result.Blockers, "inventory "+result.InventoryStatus)
	}
	if !demandFresh {
		return result, nil
	}

	assigned := min(demand.Assigned, result.EffectiveMax)
	target := assigned + min(result.EffectiveReserve, result.EffectiveMax-assigned)
	result.Target = &target
	if !inventoryFresh {
		return result, nil
	}

	unmapped := max(0, demand.Running-*result.Busy)
	result.UnmappedRunning = &unmapped
	deficit := max(0, target-*result.Serving-*result.CrediblePending-unmapped)
	result.Deficit = &deficit
	if unmapped > 0 {
		result.Blockers = append(result.Blockers, "running jobs lack matched executors")
		return result, nil
	}
	result.ProposedStarts = min(deficit, *result.Headroom)
	if result.ProposedStarts < deficit {
		result.Blockers = append(result.Blockers, "capacity limit reached")
	}
	return result, nil
}

func fresh(observedAt, now time.Time, maxAge time.Duration) bool {
	return !observedAt.After(now) && now.Sub(observedAt) <= maxAge
}

func (r *Result) observe(limits Limits, inventory Inventory, now time.Time, maxAge time.Duration) error {
	if inventory.ObservedAt.IsZero() || (inventory.BackendLimit != nil && *inventory.BackendLimit < 0) {
		return errors.New("invalid inventory observation")
	}
	isCurrent := fresh(inventory.ObservedAt, now, maxAge)
	switch {
	case !isCurrent:
		r.InventoryStatus = "stale"
	case inventory.Complete:
		r.InventoryStatus = "fresh"
	default:
		r.InventoryStatus = "partial"
	}
	if isCurrent && inventory.BackendLimit != nil {
		r.EffectiveMax = min(r.EffectiveMax, *inventory.BackendLimit)
	}
	seen := make(map[string]bool)
	var serving, busy, pending, occupied, unknown int
	for _, executor := range inventory.Executors {
		if executor.ID == "" || seen[executor.ID] {
			return errors.New("inventory requires unique executor IDs")
		}
		seen[executor.ID] = true
		switch executor.State {
		case "lost":
			continue
		case "busy", "idle", "starting", "unknown", "completed", "unused":
		default:
			return fmt.Errorf("invalid executor state for %s", executor.ID)
		}
		occupied++
		if !executor.Owned {
			unknown++
			r.OrphanCandidates = append(r.OrphanCandidates, executor.ID)
			continue
		}
		switch executor.State {
		case "busy":
			busy++
			serving++
		case "idle":
			serving++
		case "starting":
			if executor.StartedAt.IsZero() || executor.StartedAt.After(inventory.ObservedAt) {
				return fmt.Errorf("missing or invalid start time for %s", executor.ID)
			}
			if executor.StartedAt.Add(limits.StartupTimeout).After(now) {
				pending++
			} else {
				unknown++
			}
		case "unknown":
			unknown++
		case "completed", "unused":
			if r.InventoryStatus == "fresh" {
				r.CleanupCandidates = append(r.CleanupCandidates, executor.ID)
			}
		}
	}
	slices.Sort(r.OrphanCandidates)
	slices.Sort(r.CleanupCandidates)
	r.Serving, r.Busy, r.CrediblePending = &serving, &busy, &pending
	r.Occupied, r.Unknown = &occupied, &unknown
	if r.InventoryStatus == "fresh" {
		headroom := max(0, r.EffectiveMax-occupied)
		r.Headroom = &headroom
	}
	return nil
}
