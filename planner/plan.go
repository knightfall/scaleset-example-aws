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
		result.Assigned, result.Running = new(demand.Assigned), new(demand.Running)
		if fresh(demand.ObservedAt, now, maxAge) {
			result.DemandStatus = "fresh"
		}
	}
	if inventory != nil {
		if err := result.observe(limits, inventory, now, maxAge); err != nil {
			return result, err
		}
	}
	result.EffectiveReserve = min(result.EffectiveReserve, result.EffectiveMax)
	if result.DemandStatus == "fresh" {
		assigned := min(*result.Assigned, result.EffectiveMax)
		target := assigned + min(result.EffectiveReserve, result.EffectiveMax-assigned)
		result.Target = &target
	} else {
		result.Blockers = append(result.Blockers, "demand "+result.DemandStatus)
	}
	if result.InventoryStatus != "fresh" {
		result.Blockers = append(result.Blockers, "inventory "+result.InventoryStatus)
	}
	if result.Target != nil && result.InventoryStatus == "fresh" {
		unmapped := max(0, *result.Running-*result.Busy)
		result.UnmappedRunning = &unmapped
		deficit := max(0, *result.Target-*result.Serving-*result.CrediblePending-unmapped)
		result.Deficit = &deficit
		if unmapped > 0 {
			result.Blockers = append(result.Blockers, "running jobs lack matched executors")
			return result, nil
		}
		result.ProposedStarts = min(deficit, *result.Headroom)
		if result.ProposedStarts < deficit {
			result.Blockers = append(result.Blockers, "capacity limit reached")
		}
	}
	return result, nil
}

func fresh(observedAt, now time.Time, maxAge time.Duration) bool {
	return !observedAt.After(now) && now.Sub(observedAt) <= maxAge
}

func (r *Result) observe(limits Limits, inventory *Inventory, now time.Time, maxAge time.Duration) error {
	if inventory.ObservedAt.IsZero() || (inventory.BackendLimit != nil && *inventory.BackendLimit < 0) {
		return errors.New("invalid inventory observation")
	}
	r.InventoryStatus = "stale"
	if fresh(inventory.ObservedAt, now, maxAge) {
		r.InventoryStatus = "partial"
		if inventory.Complete {
			r.InventoryStatus = "fresh"
		}
		if inventory.BackendLimit != nil {
			r.EffectiveMax = min(r.EffectiveMax, *inventory.BackendLimit)
		}
	}
	seen := make(map[string]bool)
	var serving, busy, pending, occupied, unknown int
	for _, executor := range inventory.Executors {
		if executor.ID == "" || seen[executor.ID] {
			return errors.New("inventory requires unique executor IDs")
		}
		seen[executor.ID] = true
		switch executor.State {
		case "lost", "busy", "idle", "starting", "unknown", "completed", "unused":
		default:
			return fmt.Errorf("invalid executor state for %s", executor.ID)
		}
		if executor.State == "lost" {
			continue
		}
		occupied++
		if !executor.Owned {
			unknown++
			r.OrphanCandidates = append(r.OrphanCandidates, executor.ID)
			continue
		}
		switch executor.State {
		case "busy", "idle":
			serving++
			if executor.State == "busy" {
				busy++
			}
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
		r.Headroom = new(max(0, r.EffectiveMax-occupied))
	}
	return nil
}
