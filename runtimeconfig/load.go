package runtimeconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/knightfall/scaleset-example-aws/strictjson"
)

const (
	environmentSchemaID = "https://runners.example/schemas/controller-config-v1alpha1.json"
	runtimeSchemaID     = "https://runners.example/schemas/controller-runtime-v1alpha1.json"
)

// Decoder validates runtime JSON against the same schemas CDK uses.
type Decoder struct {
	schema *jsonschema.Schema
}

func NewDecoder(environmentSchema, runtimeSchema []byte) (*Decoder, error) {
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(jsonschema.SchemeURLLoader{})
	for _, input := range []struct {
		id   string
		data []byte
	}{
		{environmentSchemaID, environmentSchema},
		{runtimeSchemaID, runtimeSchema},
	} {
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(input.data))
		if err != nil {
			return nil, fmt.Errorf("read embedded schema %s: %w", input.id, err)
		}
		if err := compiler.AddResource(input.id, document); err != nil {
			return nil, fmt.Errorf("register embedded schema: %w", err)
		}
	}
	schema, err := compiler.Compile(runtimeSchemaID)
	if err != nil {
		return nil, fmt.Errorf("compile runtime schema: %w", err)
	}
	return &Decoder{schema: schema}, nil
}

// Decode validates data and, when supplied, verifies the expected SHA-256 before parsing.
func (d *Decoder) Decode(data []byte, expectedSHA256 string) (Artifact, error) {
	var artifact Artifact
	if expectedSHA256 != "" {
		want, err := hex.DecodeString(expectedSHA256)
		if err != nil || len(want) != sha256.Size {
			return artifact, errors.New("expected SHA-256 must contain 64 hexadecimal characters")
		}
		if got := sha256.Sum256(data); !bytes.Equal(got[:], want) {
			return artifact, errors.New("runtime configuration SHA-256 mismatch")
		}
	}
	if !utf8.Valid(data) {
		return artifact, errors.New("runtime JSON must use UTF-8")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	document, err := strictjson.ReadValue(decoder)
	if err != nil {
		return artifact, fmt.Errorf("invalid runtime JSON at byte %d: %w", decoder.InputOffset(), err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return artifact, errors.New("expected one runtime JSON value")
	}
	if err := d.schema.Validate(document); err != nil {
		// Schema diagnostics can contain input values. Config errors must be safe to log.
		return artifact, errors.New("runtime JSON does not match the v1alpha1 schema")
	}
	if err := json.Unmarshal(data, &artifact); err != nil {
		return Artifact{}, errors.New("runtime JSON could not be decoded into controller configuration")
	}
	if err := artifact.validate(); err != nil {
		return Artifact{}, err
	}
	if artifact.Config.Spec.Mode == "" {
		artifact.Config.Spec.Mode = "dry-run"
	}
	if artifact.Config.Spec.DryRun.Demand.Type == "" {
		artifact.Config.Spec.DryRun.Demand.Type = "none"
	}
	for i := range artifact.Config.Spec.Profiles {
		if artifact.Config.Spec.Profiles[i].GitHub.Registration == "" {
			artifact.Config.Spec.Profiles[i].GitHub.Registration = "existing"
		}
	}
	return artifact, nil
}
