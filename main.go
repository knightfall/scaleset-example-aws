package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

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
	if len(args) < 2 || args[0] != "config" || args[1] != "verify" {
		return errors.New("usage: config verify --file runtime.json [--sha256 digest]")
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
	data, err := os.ReadFile(*path)
	if err != nil {
		return fmt.Errorf("read runtime configuration: %w", err)
	}
	decoder, err := runtimeconfig.NewDecoder(environmentSchema, runtimeSchema)
	if err != nil {
		return err
	}
	artifact, err := decoder.Decode(data, *digest)
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
