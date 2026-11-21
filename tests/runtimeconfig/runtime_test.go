package runtimeconfig_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"

	"github.com/knightfall/scaleset-example-aws/awsclients"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

func read(t *testing.T, parts ...string) []byte {
	t.Helper()
	path := filepath.Join(append([]string{"..", ".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func decoder(t *testing.T) *runtimeconfig.Decoder {
	t.Helper()
	environment := read(t, "cdk", "config", "schema", "controller-config-v1alpha1.json")
	runtime := read(t, "cdk", "config", "schema", "controller-runtime-v1alpha1.json")
	result, err := runtimeconfig.NewDecoder(environment, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	return read(t, "testdata", "config", "runtime", name+".json")
}

func sum(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func replace(t *testing.T, data []byte, old, replacement string) []byte {
	t.Helper()
	if !bytes.Contains(data, []byte(old)) {
		t.Fatalf("fixture is missing %q", old)
	}
	return bytes.Replace(data, []byte(old), []byte(replacement), 1)
}

func TestLoadSharedRuntimeConfigurations(t *testing.T) {
	loader := decoder(t)
	for _, tc := range []struct {
		name      string
		host      string
		providers []string
		table     string
		clusters  int
	}{
		{"asg-only-external", "external", []string{"asg"}, "engineering-dev-runner-lifecycle", 0},
		{"fargate-only-import", "ecs-fargate", []string{"fargate"}, "existing-runner-lifecycle", 1},
		{"mixed-ecs-host", "ecs-fargate", []string{"asg", "fargate"}, "existing-runner-lifecycle", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := fixture(t, tc.name)
			got, err := loader.Decode(data, sum(data))
			if err != nil {
				t.Fatal(err)
			}
			if got.Config.Spec.Controller.Hosting.Type != tc.host ||
				got.Config.Spec.Mode != "dry-run" || got.Config.Spec.DryRun.Demand.Type != "none" ||
				got.Resources.TrackingTableName != tc.table || len(got.Resources.ECSClusterARNs) != tc.clusters ||
				!slices.Equal(got.EnabledProvisioners(), tc.providers) {
				t.Fatalf("unexpected selection: %+v", got)
			}
		})
	}
}

func TestSharedConfigurationRejections(t *testing.T) {
	var cases []struct {
		Name         string `json:"name"`
		Fixture      string `json:"fixture"`
		Path         string `json:"path"`
		Value        any    `json:"value"`
		RuntimeError string `json:"runtimeError"`
	}
	if err := json.Unmarshal(read(t, "testdata", "config", "rejections.json"), &cases); err != nil {
		t.Fatal(err)
	}
	loader := decoder(t)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			document, _ := draft(t, fixture(t, tc.Fixture))
			path := strings.Split(tc.Path, ".")
			object := document["config"].(map[string]any)
			for _, part := range path[:len(path)-1] {
				object = object[part].(map[string]any)
			}
			object[path[len(path)-1]] = tc.Value
			if _, err := loader.Decode(encode(t, document), ""); err == nil || !strings.Contains(err.Error(), tc.RuntimeError) {
				t.Fatalf("runtime returned %v, want %q", err, tc.RuntimeError)
			}
		})
	}
}

func TestRuntimeRejectsPinnedCreateIfMissing(t *testing.T) {
	data := replace(t, fixture(t, "fargate-only-import"), `"registration": "create-if-missing"`,
		`"registration": "create-if-missing", "scaleSetId": 37`)
	if _, err := decoder(t).Decode(data, ""); err == nil || !strings.Contains(err.Error(), "v1alpha1 schema") {
		t.Fatalf("pinned create-if-missing returned %v", err)
	}
}

func TestRuntimeRejectsInvalidInputBeforeCreatingClients(t *testing.T) {
	loader := decoder(t)
	original := fixture(t, "asg-only-external")
	factories := awsclients.Constructors{
		ASG: func(aws.Config) *awsclients.ASGClients {
			t.Error("created ASG clients for invalid configuration")
			return nil
		},
		Fargate: func(aws.Config) *ecs.Client {
			t.Error("created ECS client for invalid configuration")
			return nil
		},
	}
	tests := []struct {
		name, want string
		data       []byte
		digest     string
	}{
		{"digest mismatch", "SHA-256 mismatch", original, strings.Repeat("0", 64)},
		{"duplicate nested key", "duplicate JSON member", replace(t, original, `"account":`, `"account": "111122223333", "account":`), ""},
		{"unknown field", "does not match the v1alpha1 schema", replace(t, original, `"secretArn":`, `"token": "secret-fixture-value", "secretArn":`), ""},
		{"wrong discriminant", "does not match the v1alpha1 schema", replace(t, original, `"type": "asg"`, `"type": "fargate"`), ""},
		{"all disabled", "does not match the v1alpha1 schema", replace(t, original, `"state": "active"`, `"state": "disabled"`), ""},
		{"no profiles", "does not match the v1alpha1 schema", withoutProfiles(t, original), ""},
		{"reserve above max", "idleReserve exceeds maxRunners", replace(t, original, `"idleReserve": 0`, `"idleReserve": 13`), ""},
		{"scope mismatch", "incompatible with the scope", replace(t, original, `"type": "github-app"`, `"type": "enterprise-pat"`), ""},
		{"unexpected resolved table", "tracking table does not match", replace(t, original, "engineering-dev-runner-lifecycle", "incorrect-table"), ""},
		{"trailing JSON", "expected one runtime JSON value", append(bytes.Clone(original), []byte("{}")...), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := factories.Load(loader, tc.data, tc.digest, aws.Config{})
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret-fixture-value") {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func withoutProfiles(t *testing.T, data []byte) []byte {
	t.Helper()
	doc, spec := draft(t, data)
	spec["profiles"] = []any{}
	return encode(t, doc)
}

func TestAWSConstructorsFollowEnabledProfilesAndMode(t *testing.T) {
	loader := decoder(t)
	original := fixture(t, "asg-only-external")
	fargate := fixture(t, "fargate-only-import")
	mixed := fixture(t, "mixed-ecs-host")
	withReadAccess := func(input []byte) []byte {
		return replace(t, input, `"inspectAws": false`, `"inspectAws": true`)
	}
	doc, spec := draft(t, original)
	_, fargateSpec := draft(t, fargate)
	disabled := fargateSpec["profiles"].([]any)[0].(map[string]any)
	disabled["state"] = "disabled"
	spec["profiles"] = append(spec["profiles"].([]any), disabled)
	disabledFargate := encode(t, doc)

	doc, spec = draft(t, mixed)
	spec["profiles"] = spec["profiles"].([]any)[:1]
	resolved := doc["resources"].(map[string]any)["ecsClusterArns"].(map[string]any)
	delete(resolved, "runner-new")
	ecsHostedASG := encode(t, doc)

	for _, tc := range []struct {
		name  string
		data  []byte
		calls []string
	}{
		{"default dry-run", original, nil},
		{"ASG only", withReadAccess(original), []string{"asg"}},
		{"Fargate-only", withReadAccess(fargate), []string{"fargate"}},
		{"mixed", withReadAccess(mixed), []string{"asg", "fargate"}},
		{"disabled Fargate", withReadAccess(disabledFargate), []string{"asg"}},
		{"ECS host without Fargate runners", withReadAccess(ecsHostedASG), []string{"asg"}},
		{"active ASG", replace(t, original, `"mode": "dry-run"`, `"mode": "active"`), []string{"asg"}},
		{"draining ASG", withReadAccess(replace(t, original, `"state": "active"`, `"state": "draining"`)), []string{"asg"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			record := func(cfg aws.Config, kind string) {
				t.Helper()
				if cfg.Region != "ap-southeast-2" {
					t.Fatalf("%s initialized for wrong Region: %s", kind, cfg.Region)
				}
				calls = append(calls, kind)
			}
			factories := awsclients.Constructors{
				ASG: func(cfg aws.Config) *awsclients.ASGClients {
					record(cfg, "asg")
					return &awsclients.ASGClients{}
				},
				Fargate: func(cfg aws.Config) *ecs.Client {
					record(cfg, "fargate")
					return &ecs.Client{}
				},
			}
			_, clients, err := factories.Load(loader, tc.data, sum(tc.data), aws.Config{Region: "us-east-1"})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(calls, tc.calls) {
				t.Fatalf("SDK constructors called = %v, want %v", calls, tc.calls)
			}
			if (clients.ASG != nil) != slices.Contains(tc.calls, "asg") ||
				(clients.ECS != nil) != slices.Contains(tc.calls, "fargate") {
				t.Fatalf("unexpected clients: %+v", clients)
			}
		})
	}
}

func TestDefaultConstructorsUseOnlySelectedSDKFamilies(t *testing.T) {
	data := replace(t, fixture(t, "asg-only-external"), `"inspectAws": false`, `"inspectAws": true`)
	_, clients, err := (awsclients.Constructors{}).Load(decoder(t), data, sum(data), aws.Config{})
	if err != nil || clients.ASG == nil || clients.ASG.Scaling == nil || clients.ASG.Inventory == nil || clients.ECS != nil {
		t.Fatalf("unexpected SDK client selection: %+v, error: %v", clients, err)
	}
}

func draft(t *testing.T, data []byte) (map[string]any, map[string]any) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	return doc, doc["config"].(map[string]any)["spec"].(map[string]any)
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return data
}
