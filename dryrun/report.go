// Package dryrun reconciles file-based observations without AWS or GitHub clients.
package dryrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/knightfall/scaleset-example-aws/planner"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

const MaxObservationAge = 10 * time.Minute

type DemandFile struct {
	APIVersion  string   `json:"apiVersion"`
	Kind        string   `json:"kind"`
	Environment string   `json:"environment"`
	Records     []Record `json:"records"`
}

type Record struct {
	ProfileID   string      `json:"profileId"`
	ScopeURL    string      `json:"scopeURL"`
	RunnerGroup string      `json:"runnerGroup"`
	ScaleSet    string      `json:"scaleSet"`
	ScaleSetID  int64       `json:"scaleSetId,omitempty"`
	ObservedAt  time.Time   `json:"observedAt"`
	Demand      *Statistics `json:"demand,omitempty"`
	Inventory   *Inventory  `json:"inventory,omitempty"`
}

type Statistics struct {
	Assigned *int `json:"assignedJobs"`
	Running  *int `json:"runningJobs"`
}

type Inventory struct {
	Complete     bool               `json:"complete"`
	BackendLimit *int               `json:"backendLimit,omitempty"`
	Executors    []planner.Executor `json:"executors"`
}

type Report struct {
	Environment string          `json:"environment"`
	Mode        string          `json:"mode"`
	Source      string          `json:"source"`
	AsOf        time.Time       `json:"asOf"`
	Advisory    bool            `json:"advisory"`
	Limitations []string        `json:"limitations,omitempty"`
	Profiles    []ProfileReport `json:"profiles"`
}

type ProfileReport struct {
	ID                  string     `json:"id"`
	Provisioner         string     `json:"provisioner"`
	ScaleSet            string     `json:"scaleSet"`
	DemandObservedAt    *time.Time `json:"demandObservedAt"`
	InventoryObservedAt *time.Time `json:"inventoryObservedAt"`
	planner.Result
}

type observations struct {
	demand    *planner.Demand
	inventory *planner.Inventory
}

// Reconcile calculates an advisory report. source is nil when no demand file is selected.
func Reconcile(artifact runtimeconfig.Artifact, source io.Reader, at time.Time) (Report, error) {
	config := artifact.Config
	input := config.Spec.DryRun.Demand.Type
	report := Report{
		Environment: config.Metadata.Name, Mode: "dry-run", Source: input, AsOf: at.UTC(), Advisory: true,
		Profiles: []ProfileReport{},
	}
	if config.Spec.DryRun.InspectAWS {
		report.Limitations = append(report.Limitations, "offline dry-run did not inspect live AWS resources")
	}
	profiles := make(map[string]runtimeconfig.Profile, len(config.Spec.Profiles))
	for _, profile := range config.Spec.Profiles {
		profiles[profile.ID] = profile
	}

	state := make(map[string]observations)
	if source != nil {
		snapshot, err := readSource(source, input, report.Environment)
		if err != nil {
			return report, err
		}
		if input == "replay" {
			slices.SortStableFunc(snapshot.Records, func(a, b Record) int { return a.ObservedAt.Compare(b.ObservedAt) })
			report.AsOf = snapshot.Records[len(snapshot.Records)-1].ObservedAt.UTC()
		}
		for _, record := range snapshot.Records {
			profile, ok := profiles[record.ProfileID]
			if !ok {
				return report, fmt.Errorf("demand source references unknown profile %s", record.ProfileID)
			}
			connection := config.Spec.GitHub[profile.GitHub.Connection]
			if profile.State == "disabled" {
				continue
			}
			if err := record.verify(profile, connection); err != nil {
				return report, fmt.Errorf("profile %s: %w", profile.ID, err)
			}
			record.ObservedAt = record.ObservedAt.UTC()
			previous := state[profile.ID]
			err := previous.apply(record, input)
			if err != nil {
				return report, fmt.Errorf("profile %s: %w", profile.ID, err)
			}
			if input == "replay" {
				if _, err := planner.Calculate(limits(profile), previous.demand, previous.inventory, record.ObservedAt, MaxObservationAge); err != nil {
					return report, fmt.Errorf("replay profile %s: %w", profile.ID, err)
				}
			}
			state[profile.ID] = previous
		}
	} else if input != "none" {
		return report, errors.New("configured demand source could not be read")
	}

	for _, profile := range config.Spec.Profiles {
		if profile.State == "disabled" {
			continue
		}
		evidence := state[profile.ID]
		result, err := planner.Calculate(limits(profile), evidence.demand, evidence.inventory, report.AsOf, MaxObservationAge)
		if err != nil {
			return report, fmt.Errorf("plan profile %s: %w", profile.ID, err)
		}
		entry := ProfileReport{
			ID: profile.ID, Provisioner: profile.Provisioner.Type, ScaleSet: profile.GitHub.ScaleSet, Result: result,
		}
		if evidence.demand != nil {
			entry.DemandObservedAt = new(evidence.demand.ObservedAt)
		}
		if evidence.inventory != nil {
			entry.InventoryObservedAt = new(evidence.inventory.ObservedAt)
		}
		report.Profiles = append(report.Profiles, entry)
	}
	return report, nil
}

func limits(profile runtimeconfig.Profile) planner.Limits {
	return planner.Limits{
		MaxRunners: profile.Capacity.MaxRunners, IdleReserve: profile.Capacity.IdleReserve,
		Draining: profile.State == "draining", StartupTimeout: time.Duration(profile.Capacity.StartupTimeoutSeconds) * time.Second,
	}
}

func readSource(r io.Reader, source, environment string) (DemandFile, error) {
	var document DemandFile
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return document, fmt.Errorf("invalid demand source: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return document, errors.New("expected one demand source document")
	}
	if document.APIVersion != "runners.example/v1alpha1" || document.Environment != environment ||
		len(document.Records) == 0 || (source == "snapshot" && document.Kind != "RunnerDemandSnapshot") ||
		(source == "replay" && document.Kind != "RunnerDemandReplay") || (source != "snapshot" && source != "replay") {
		return document, errors.New("demand source version, kind, environment or records do not match the configuration")
	}
	return document, nil
}

func (r Record) verify(profile runtimeconfig.Profile, connection runtimeconfig.GitHubConnection) error {
	binding := profile.GitHub
	if !strings.EqualFold(strings.TrimSuffix(r.ScopeURL, "/"), strings.TrimSuffix(connection.ScopeURL, "/")) ||
		!strings.EqualFold(r.RunnerGroup, binding.RunnerGroup) ||
		!strings.EqualFold(r.ScaleSet, binding.ScaleSet) ||
		(binding.ScaleSetID != 0 && r.ScaleSetID != binding.ScaleSetID) || r.ScaleSetID < 0 {
		return errors.New("demand source belongs to a different scale set")
	}
	if r.ObservedAt.IsZero() || (r.Demand == nil && r.Inventory == nil) {
		return errors.New("demand record requires time and observations")
	}
	if r.Demand != nil && (r.Demand.Assigned == nil || r.Demand.Running == nil ||
		*r.Demand.Assigned < 0 || *r.Demand.Running < 0 || *r.Demand.Running > *r.Demand.Assigned) {
		return errors.New("demand record requires valid assigned and running counts")
	}
	return nil
}

func (o *observations) apply(r Record, source string) error {
	if source == "snapshot" && (o.demand != nil || o.inventory != nil) {
		return errors.New("snapshot contains multiple records for the same profile")
	}
	if r.Demand != nil {
		next := planner.Demand{ObservedAt: r.ObservedAt, Assigned: *r.Demand.Assigned, Running: *r.Demand.Running}
		if o.demand != nil && r.ObservedAt.Equal(o.demand.ObservedAt) &&
			(next.Assigned != o.demand.Assigned || next.Running != o.demand.Running) {
			return errors.New("conflicting demand records at the same time")
		}
		o.demand = &next
	}
	if r.Inventory != nil {
		next := planner.Inventory{
			ObservedAt: r.ObservedAt, Complete: r.Inventory.Complete, BackendLimit: r.Inventory.BackendLimit,
			Executors: r.Inventory.Executors,
		}
		if o.inventory != nil && r.ObservedAt.Equal(o.inventory.ObservedAt) &&
			!reflect.DeepEqual(o.inventory, &next) {
			return errors.New("conflicting inventory records at the same time")
		}
		o.inventory = &next
	}
	return nil
}
