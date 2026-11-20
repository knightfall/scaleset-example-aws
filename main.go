package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/knightfall/scaleset-example-aws/dryrun"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

//go:embed cdk/config/schema/controller-config-v1alpha1.json
var environmentSchema []byte

//go:embed cdk/config/schema/controller-runtime-v1alpha1.json
var runtimeSchema []byte

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "reconcile" {
		return reconcileOnce(args[1:], stdout, stderr)
	}
	if len(args) < 2 || args[0] != "config" || args[1] != "verify" {
		return errors.New("usage: config verify --file runtime.json or reconcile --config runtime.json --dry-run --once")
	}
	flags := flag.NewFlagSet("config verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("file", "", "runtime JSON file to validate")
	digest := flags.String("sha256", "", "expected SHA-256 of the runtime JSON")
	if err := flags.Parse(args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return errors.New("config verify requires --file and accepts no positional arguments")
	}
	artifact, data, err := loadRuntimeArtifact(*path, *digest)
	if err != nil {
		return err
	}
	sha := sha256.Sum256(data)
	return json.NewEncoder(stdout).Encode(struct {
		Environment  string   `json:"environment"`
		Mode         string   `json:"mode"`
		Provisioners []string `json:"provisioners"`
		SHA256       string   `json:"sha256"`
	}{
		Environment:  artifact.Config.Metadata.Name,
		Mode:         artifact.Config.Spec.Mode,
		Provisioners: artifact.EnabledProvisioners(),
		SHA256:       hex.EncodeToString(sha[:]),
	})
}

func reconcileOnce(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "", "runtime JSON file to read")
	digest := flags.String("sha256", "", "expected SHA-256 of the runtime JSON")
	forceDryRun := flags.Bool("dry-run", false, "plan only, including for an active configuration")
	once := flags.Bool("once", true, "emit one advisory report")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *path == "" || flags.NArg() != 0 || !*once {
		return errors.New("reconcile requires --config and runs one report at a time")
	}
	artifact, _, err := loadRuntimeArtifact(*path, *digest)
	if err != nil {
		return err
	}
	if artifact.Config.Spec.Mode == "active" && !*forceDryRun {
		return errors.New("reconcile currently requires dry-run mode or --dry-run")
	}
	input := artifact.Config.Spec.DryRun.Demand
	var source io.Reader
	if input.Type != "none" {
		if input.Path == "" {
			return errors.New("offline dry-run requires a local snapshot or replay path")
		}
		observations, err := os.ReadFile(input.Path)
		if err != nil {
			return fmt.Errorf("read demand source: %w", err)
		}
		source = bytes.NewReader(observations)
	}
	report, err := dryrun.Reconcile(artifact, source, time.Now())
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(report)
}

func loadRuntimeArtifact(path, expectedSHA256 string) (runtimeconfig.Artifact, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return runtimeconfig.Artifact{}, nil, fmt.Errorf("read runtime configuration: %w", err)
	}
	decoder, err := runtimeconfig.NewDecoder(environmentSchema, runtimeSchema)
	if err != nil {
		return runtimeconfig.Artifact{}, nil, err
	}
	artifact, err := decoder.Decode(data, expectedSHA256)
	return artifact, data, err
}
