package dryrun_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/knightfall/scaleset-example-aws/dryrun"
	"github.com/knightfall/scaleset-example-aws/planner"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

var now = time.Date(2026, time.November, 21, 0, 3, 0, 0, time.UTC)

func read(t *testing.T, parts ...string) []byte {
	t.Helper()
	file := filepath.Join(append([]string{"..", ".."}, parts...)...)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func artifact(t *testing.T, input string) runtimeconfig.Artifact {
	t.Helper()
	decoder, err := runtimeconfig.NewDecoder(
		read(t, "cdk", "config", "schema", "controller-config-v1alpha1.json"),
		read(t, "cdk", "config", "schema", "controller-runtime-v1alpha1.json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decoder.Decode(read(t, "testdata", "config", "runtime", "asg-only-external.json"), "")
	if err != nil {
		t.Fatal(err)
	}
	value.Config.Spec.DryRun.Demand.Type = input
	return value
}

func record(when time.Time, assigned int) dryrun.Record {
	return dryrun.Record{
		ProfileID: "linux-docker", ScopeURL: "https://github.com/example-engineering",
		RunnerGroup: "Linux-Docker", ScaleSet: "linux-docker", ObservedAt: when,
		Demand: &dryrun.Statistics{Assigned: new(assigned), Running: new(0)},
	}
}

func source(t *testing.T, kind string, records ...dryrun.Record) *bytes.Reader {
	t.Helper()
	data, err := json.Marshal(dryrun.DemandFile{
		APIVersion: "runners.example/v1alpha1", Kind: kind, Environment: "engineering-dev", Records: records,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(data)
}

func TestRepositoryReplayFixture(t *testing.T) {
	input := bytes.NewReader(read(t, "testdata", "dryrun", "replay.json"))
	report, err := dryrun.Reconcile(artifact(t, "replay"), input, now)
	if err != nil || len(report.Profiles) != 1 {
		t.Fatalf("report = %+v, error = %v", report, err)
	}
	profile := report.Profiles[0]
	if *profile.Target != 3 || *profile.Deficit != 1 || profile.ProposedStarts != 1 ||
		!slices.Equal(profile.OrphanCandidates, []string{"fixture-unmapped"}) {
		t.Fatalf("unexpected replay fixture report: %+v", profile)
	}
}

func TestNoDemandInputKeepsCapacityUnknown(t *testing.T) {
	config := artifact(t, "none")
	config.Config.Spec.DryRun.InspectAWS = true
	report, err := dryrun.Reconcile(config, nil, now)
	if err != nil || len(report.Profiles) != 1 {
		t.Fatalf("report = %+v, error = %v", report, err)
	}
	profile := report.Profiles[0]
	if !report.Advisory || profile.Target != nil || profile.InventoryStatus != "unknown" ||
		profile.DemandStatus != "unknown" || profile.ProposedStarts != 0 ||
		!slices.Contains(report.Limitations, "offline dry-run did not inspect live AWS resources") {
		t.Fatalf("unexpected unknown-demand report: %+v", report)
	}
}

func TestSnapshotReportsHeadroomAndOrphans(t *testing.T) {
	observation := record(now, 4)
	observation.Inventory = &dryrun.Inventory{Complete: true, BackendLimit: new(3), Executors: []planner.Executor{
		{ID: "unmapped", State: "unknown"}, {ID: "ready", State: "idle", Owned: true},
	}}
	report, err := dryrun.Reconcile(artifact(t, "snapshot"), source(t, "RunnerDemandSnapshot", observation), now)
	if err != nil || len(report.Profiles) != 1 {
		t.Fatalf("report = %+v, error = %v", report, err)
	}
	profile := report.Profiles[0]
	if *profile.Target != 3 || *profile.Deficit != 2 || *profile.Headroom != 1 || profile.ProposedStarts != 1 ||
		!slices.Equal(profile.OrphanCandidates, []string{"unmapped"}) {
		t.Fatalf("unexpected profile report: %+v", profile)
	}
}

func TestReplayUsesChronologicalVirtualTimeAndLatestEvidence(t *testing.T) {
	initial := record(now.Add(-time.Hour), 1)
	initial.Inventory = &dryrun.Inventory{Complete: true, Executors: []planner.Executor{}}
	latest := record(now, 2)
	latest.Inventory = &dryrun.Inventory{Complete: true, Executors: []planner.Executor{{ID: "ready", State: "idle", Owned: true}}}
	config := artifact(t, "replay")
	report, err := dryrun.Reconcile(config, source(t, "RunnerDemandReplay", latest, initial), now.Add(30*time.Hour))
	if err != nil || len(report.Profiles) != 1 {
		t.Fatalf("report = %+v, error = %v", report, err)
	}
	if !report.AsOf.Equal(now) || !report.Profiles[0].DemandObservedAt.Equal(now) ||
		*report.Profiles[0].Assigned != 2 || report.Profiles[0].ProposedStarts != 1 {
		t.Fatalf("unexpected replay: %+v", report)
	}
}

func TestRejectsMismatchedAndConflictingInput(t *testing.T) {
	valid := record(now, 1)
	mismatched := valid
	mismatched.ScaleSet = "wrong"
	invalidCounts := valid
	invalidCounts.Demand = &dryrun.Statistics{Assigned: new(0)}
	for _, tc := range []struct {
		mode, kind, message string
		records             []dryrun.Record
	}{
		{"snapshot", "RunnerDemandSnapshot", "different scale set", []dryrun.Record{mismatched}},
		{"snapshot", "RunnerDemandSnapshot", "assigned and running counts", []dryrun.Record{invalidCounts}},
		{"snapshot", "RunnerDemandSnapshot", "multiple records", []dryrun.Record{valid, valid}},
		{"replay", "RunnerDemandReplay", "conflicting demand", []dryrun.Record{valid, record(now, 2)}},
		{"snapshot", "RunnerDemandReplay", "kind", []dryrun.Record{valid}},
	} {
		t.Run(tc.message, func(t *testing.T) {
			_, err := dryrun.Reconcile(artifact(t, tc.mode), source(t, tc.kind, tc.records...), now)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v, want %q", err, tc.message)
			}
		})
	}
}
