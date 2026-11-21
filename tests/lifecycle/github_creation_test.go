package lifecycle_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type fixtureSecret struct{}

func (fixtureSecret) GetSecretValue(_ context.Context, input *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return &secretsmanager.GetSecretValueOutput{
		ARN: input.SecretId, SecretString: aws.String(`{"token":"fixture-pat"}`), VersionId: aws.String("fixture-version"),
	}, nil
}

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func githubCreationConfig() runtimeconfig.Artifact {
	var config runtimeconfig.Artifact
	config.Config.Metadata.Name = "dev"
	config.Config.Spec = runtimeconfig.Spec{
		Mode: "active",
		GitHub: map[string]runtimeconfig.GitHubConnection{
			"build": {ScopeURL: "https://github.com/enterprises/fixture", Auth: runtimeconfig.GitHubAuth{
				Type: "enterprise-pat", SecretARN: "arn:aws:secretsmanager:ap-southeast-2:111122223333:secret:github-fixture-ABC123",
			}},
		},
		Profiles: []runtimeconfig.Profile{{ID: "build", State: "active", GitHub: runtimeconfig.GitHubBinding{
			Connection: "build", RunnerGroup: "Build", ScaleSet: "linux",
			Labels: []string{"linux"}, Registration: "create-if-missing",
		}}},
	}
	return config
}

func TestScaleSetCreationPersistsIntentBeforeOneWriteAndConfirmsReadback(t *testing.T) {
	for _, loseResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("lose-response-%t", loseResponse), func(t *testing.T) {
			ledger, _ := store(t)
			lease, err := ledger.AcquireLease(t.Context(), "dev", "controller", time.Now(), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			const profile, effectID = "build", "first-create"
			var posts, readbacks int
			transport := fixtureTransport(func(req *http.Request) (*http.Response, error) {
				t.Helper()
				if req.Body != nil {
					defer func() {
						if err := req.Body.Close(); err != nil {
							t.Errorf("close fixture request: %v", err)
						}
					}()
				}
				status := http.StatusOK
				var body string
				switch req.URL.Path {
				case "/enterprises/fixture/actions/runners/registration-token":
					status = http.StatusCreated
					body = `{"token":"registration-fixture","expires_at":"2099-01-01T00:00:00Z"}`
				case "/actions/runner-registration":
					payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
					token := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fixture"
					body = fmt.Sprintf(`{"url":"https://actions.example.invalid","token":%q}`, token)
				case "/_apis/runtime/runnergroups/":
					body = `{"count":1,"value":[{"id":19,"name":"Build"}]}`
				case "/_apis/runtime/runnerscalesets":
					if req.Method == http.MethodPost {
						posts++
						claim, err := ledger.Get(t.Context(), lifecycle.ClaimKey("github-set-name:https://github.com/enterprises/fixture#19#linux"))
						if err != nil || claim == nil || claim.Deployment != lease.Deployment || string(claim.Payload) != `"build"` {
							t.Fatalf("profile name claim before POST = %+v, error = %v", claim, err)
						}
						effect, err := ledger.Effect(t.Context(), "dev", profile, effectID)
						if err != nil || effect == nil || effect.State != lifecycle.EffectExecuting ||
							effect.Kind != "create-scale-set" || len(effect.RequestSHA256) != 64 {
							t.Fatalf("journal before POST = %+v, error = %v", effect, err)
						}
						var request struct {
							Name          string
							RunnerGroupID int
							Labels        []struct{ Name, Type string }
							RunnerSetting struct{ DisableUpdate bool }
						}
						if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
							t.Fatal(err)
						}
						if request.Name != "linux" || request.RunnerGroupID != 19 || !request.RunnerSetting.DisableUpdate ||
							len(request.Labels) != 1 || request.Labels[0].Name != "linux" || request.Labels[0].Type != "System" {
							t.Fatalf("wrong scale set creation request: %+v", request)
						}
						if loseResponse {
							return nil, errors.New("fixture-broker-lost-response")
						}
						body = `{"id":7}`
						break
					}
					body = `{"count":0,"value":[]}`
					if posts > 0 && !loseResponse {
						readbacks++
						body = `{"count":1,"value":[{"id":7,"name":"linux","runnerGroupId":19,"labels":[{"name":"linux"}]}]}`
					}
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
				}
				return &http.Response{
					StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(body)), Request: req,
				}, nil
			})
			loader := githubsets.Loader{
				Secrets: fixtureSecret{}, WrapTransport: func(http.RoundTripper) http.RoundTripper { return transport },
			}
			connection, err := loader.Load(ctx, githubCreationConfig(), profile)
			if err != nil {
				t.Fatal(err)
			}
			request := githubsets.CreateRequest{ProfileID: profile, EffectID: effectID, Lease: lease}
			set, err := connection.Create(ctx, ledger, request)
			if posts != 1 {
				t.Fatalf("creation attempted %d POSTs, want one", posts)
			}
			effect, readErr := ledger.Effect(t.Context(), "dev", profile, effectID)
			if readErr != nil || effect == nil {
				t.Fatalf("journal effect = %+v, error = %v", effect, readErr)
			}
			if loseResponse {
				if !errors.Is(err, githubsets.ErrCreateUnconfirmed) || strings.Contains(err.Error(), "fixture-broker") ||
					readbacks != 0 || effect.State != lifecycle.EffectExecuting {
					t.Fatalf("lost outcome = %v, readbacks = %d, effect = %+v", err, readbacks, effect)
				}
				if _, err := connection.Create(ctx, ledger, request); !errors.Is(err, lifecycle.ErrEffectNotReady) || posts != 1 {
					t.Fatalf("unresolved effect was retried, error = %v, POSTs = %d", err, posts)
				}
				return
			}
			if err != nil || set != (githubsets.ScaleSet{ID: 7, GroupID: 19}) ||
				readbacks != 1 || effect.State != lifecycle.EffectConfirmed || effect.Evidence != "scale-set:7" {
				t.Fatalf("successful outcome = %+v, error = %v, readbacks = %d, effect = %+v", set, err, readbacks, effect)
			}
		})
	}
}

func TestScaleSetCreationRequiresExplicitUnpinnedActiveProfile(t *testing.T) {
	for _, tc := range []struct {
		state, registration string
		pinned              int64
	}{
		{"active", "existing", 0},
		{"active", "create-if-missing", 7},
		{"draining", "create-if-missing", 0},
	} {
		config := githubCreationConfig()
		profile := &config.Config.Spec.Profiles[0]
		profile.State, profile.GitHub.Registration, profile.GitHub.ScaleSetID = tc.state, tc.registration, tc.pinned
		loader := githubsets.Loader{
			Secrets: fixtureSecret{},
			WrapTransport: func(http.RoundTripper) http.RoundTripper {
				return fixtureTransport(func(req *http.Request) (*http.Response, error) {
					t.Fatalf("creation used GitHub for profile %v: %s", tc, req.Method)
					return nil, nil
				})
			},
		}
		connection, err := loader.Load(t.Context(), config, "build")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Create(t.Context(), nil, githubsets.CreateRequest{ProfileID: "build"}); err == nil ||
			!strings.Contains(err.Error(), "explicitly allow creation") {
			t.Fatalf("profile %v did not reject creation: %v", tc, err)
		}
	}
}
