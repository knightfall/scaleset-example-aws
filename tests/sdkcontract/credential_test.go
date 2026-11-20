package sdkcontract

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

const fixtureSecretARN = "arn:aws:secretsmanager:ap-southeast-2:111122223333:secret:github-fixture-ABC123"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func credentialArtifact(kind, scope string) runtimeconfig.Artifact {
	var artifact runtimeconfig.Artifact
	artifact.Config.Metadata.Name = "fixture"
	artifact.Config.Spec = runtimeconfig.Spec{
		Mode: "active",
		GitHub: map[string]runtimeconfig.GitHubConnection{
			"build": {ScopeURL: scope, Auth: runtimeconfig.GitHubAuth{Type: kind, SecretARN: fixtureSecretARN}},
		},
		Profiles: []runtimeconfig.Profile{
			{ID: "linux", State: "active", GitHub: runtimeconfig.GitHubBinding{
				Connection: "build", RunnerGroup: "Build Group", ScaleSet: "linux-x64", ScaleSetID: 7,
				Labels: []string{"linux-x64", "docker-linux"}, Registration: "existing",
			}},
			{ID: "retired", State: "disabled", GitHub: runtimeconfig.GitHubBinding{Connection: "build"}},
		},
	}
	return artifact
}

func secretExchange(t *testing.T, version, document string) exchange {
	t.Helper()
	return exchange{
		method: "POST", path: "/", status: http.StatusOK,
		response: fmt.Sprintf(`{"ARN":%q,"VersionId":%q,"SecretString":%q}`, fixtureSecretARN, version, document),
		check: func(r *http.Request) {
			checkValue(t, "Secrets Manager operation", r.Header.Get("X-Amz-Target"), "secretsmanager.GetSecretValue")
			checkJSON(t, r, `{"SecretId":"`+fixtureSecretARN+`","VersionStage":"AWSCURRENT"}`)
		},
	}
}

func credentialLoader(secrets, github *scriptedHTTP) githubsets.Loader {
	return githubsets.Loader{
		Secrets: secretsmanager.NewFromConfig(awsConfig(secrets)),
		WrapTransport: func(http.RoundTripper) http.RoundTripper {
			return roundTripFunc(github.Do)
		},
	}
}

func groupLookup(t *testing.T) exchange {
	t.Helper()
	return exchange{
		method: "GET", path: "/_apis/runtime/runnergroups/", status: http.StatusOK,
		response: `{"count":1,"value":[{"id":19,"name":"Build Group"}]}`,
		check: func(r *http.Request) {
			checkValue(t, "queried group name", r.URL.Query().Get("groupName"), "Build Group")
		},
	}
}

func setLookup(t *testing.T, response string) exchange {
	t.Helper()
	return exchange{
		method: "GET", path: scaleSetPath, status: http.StatusOK, response: response,
		check: func(r *http.Request) {
			checkValue(t, "queried group ID", r.URL.Query().Get("runnerGroupId"), "19")
			checkValue(t, "queried set name", r.URL.Query().Get("name"), "linux-x64")
		},
	}
}

const matchingSetResponse = `{"count":1,"value":[{"id":7,"name":"linux-x64","runnerGroupId":19,"labels":[{"name":"Docker-Linux"},{"name":"linux-x64"}]}]}`

func TestConfiguredEnterprisePATLoadsAndResolvesOnlySelectedSets(t *testing.T) {
	const scope = "https://corp.ghe.com/enterprises/corp"
	cfg := credentialArtifact("enterprise-pat", scope)
	github := newScript(t,
		registrationExchange(t, "api.corp.ghe.com", "/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
		adminExchange(t, "api.corp.ghe.com", "/actions/runner-registration", scope),
		groupLookup(t), setLookup(t, matchingSetResponse),
		groupLookup(t), setLookup(t, matchingSetResponse),
	)
	loader := credentialLoader(newScript(t, secretExchange(t, "pat-v1", `{"token":"fixture-pat"}`)), github)
	connection, err := loader.Load(t.Context(), cfg, "build")
	if err != nil {
		t.Fatal(err)
	}
	if connection.VersionID != "pat-v1" {
		t.Fatalf("version = %q", connection.VersionID)
	}
	if err := connection.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
	set, err := connection.Lookup(t.Context(), "linux")
	if err != nil || set != (githubsets.ScaleSet{ID: 7, GroupID: 19}) {
		t.Fatalf("set = %+v, error = %v", set, err)
	}
	if _, err := connection.Lookup(t.Context(), "retired"); err == nil {
		t.Fatal("disabled profile was accepted")
	}
}

func TestConfiguredGitHubAppUsesInstallationAndRepositoryScope(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	document, err := json.Marshal(map[string]any{"clientId": "Iv1.fixture", "installationId": 42, "privateKey": string(privateKey)})
	if err != nil {
		t.Fatal(err)
	}
	const scope = "https://github.com/example/build"
	github := newScript(t,
		exchange{method: "POST", path: "/app/installations/42/access_tokens", status: http.StatusCreated,
			response: `{"token":"installation-fixture","expires_at":"2099-01-01T00:00:00Z"}`,
			check: func(r *http.Request) {
				checkValue(t, "App host", r.URL.Host, "api.github.com")
				if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "Bearer ") || strings.Count(auth, ".") != 2 {
					t.Error("app exchange did not send a bearer JWT")
				}
			}},
		registrationExchange(t, "api.github.com", "/repos/example/build/actions/runners/registration-token", "Bearer installation-fixture"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		groupLookup(t), setLookup(t, matchingSetResponse),
	)
	loader := credentialLoader(newScript(t, secretExchange(t, "app-v1", string(document))), github)
	connection, err := loader.Load(t.Context(), credentialArtifact("github-app", scope), "build")
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubCredentialsAreSkippedForDryRunAndUnselectedConnections(t *testing.T) {
	cfg := credentialArtifact("enterprise-pat", "https://github.com/enterprises/corp")
	loader := credentialLoader(newScript(t), newScript(t))
	cfg.Config.Spec.Mode = "dry-run"
	cfg.Config.Spec.DryRun.InspectAWS = true
	if _, err := loader.Load(t.Context(), cfg, "build"); !errors.Is(err, githubsets.ErrInactive) {
		t.Fatalf("dry-run error = %v", err)
	}
	cfg.Config.Spec.Mode = "active"
	cfg.Config.Spec.Profiles[0].State = "disabled"
	if _, err := loader.Load(t.Context(), cfg, "build"); !errors.Is(err, githubsets.ErrUnusedConnection) {
		t.Fatalf("disabled connection error = %v", err)
	}
}

func TestGitHubCredentialCandidateRejectionLeavesExistingClientUsable(t *testing.T) {
	const scope = "https://github.com/enterprises/corp"
	const tokenPath = "/enterprises/corp/actions/runners/registration-token"
	cfg := credentialArtifact("enterprise-pat", scope)
	github := newScript(t,
		registrationExchange(t, "api.github.com", tokenPath, "Bearer fixture-current"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		groupLookup(t), setLookup(t, matchingSetResponse),
		exchange{method: "POST", path: tokenPath, status: http.StatusForbidden,
			response: `{"message":"fixture-candidate secret in remote body"}`},
		groupLookup(t), setLookup(t, matchingSetResponse),
	)
	loader := credentialLoader(newScript(t,
		secretExchange(t, "v1", `{"token":"fixture-current"}`), secretExchange(t, "v2", `{"token":"fixture-candidate"}`),
	), github)
	current, err := loader.Load(t.Context(), cfg, "build")
	if err != nil {
		t.Fatal(err)
	}
	if err := current.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
	candidate, err := loader.Load(t.Context(), cfg, "build")
	if err != nil {
		t.Fatal(err)
	}
	if current.VersionID != "v1" || candidate.VersionID != "v2" {
		t.Fatalf("current = %q, candidate = %q", current.VersionID, candidate.VersionID)
	}
	err = candidate.Validate(t.Context())
	if !errors.Is(err, githubsets.ErrUnauthorized) || strings.Contains(err.Error(), "fixture-candidate") {
		t.Fatalf("expected redacted authorization error, got %v", err)
	}
	if _, err := current.Lookup(t.Context(), "linux"); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubCredentialRateLimitPreservesSafeBackoff(t *testing.T) {
	cfg := credentialArtifact("enterprise-pat", "https://github.com/enterprises/corp")
	// The SDK recognizes the secondary limit from the response message.
	github := newScript(t, exchange{method: "POST", path: "/enterprises/corp/actions/runners/registration-token",
		status: http.StatusForbidden, response: `{"message":"secondary rate limit includes fixture-token"}`,
		headers: http.Header{"Retry-After": []string{"17"}}})
	loader := credentialLoader(newScript(t, secretExchange(t, "v1", `{"token":"fixture-token"}`)), github)
	connection, err := loader.Load(t.Context(), cfg, "build")
	if err != nil {
		t.Fatal(err)
	}
	err = connection.Validate(t.Context())
	backoff, ok := errors.AsType[*githubsets.RateLimitError](err)
	if !ok || backoff.RetryAfter != 17*time.Second || errors.Is(err, githubsets.ErrUnauthorized) ||
		strings.Contains(err.Error(), "fixture-token") {
		t.Fatalf("expected redacted rate-limit error with retry delay, got %v", err)
	}
}

func TestGitHubLookupDistinguishesMissingAndDrift(t *testing.T) {
	const scope = "https://github.com/enterprises/corp"
	cfg := credentialArtifact("enterprise-pat", scope)
	for _, tt := range []struct {
		name     string
		response string
		want     error
	}{
		{"missing", `{"count":0,"value":[]}`, githubsets.ErrScaleSetMissing},
		{"extra label", strings.Replace(matchingSetResponse, `{"name":"Docker-Linux"}`, `{"name":"Docker-Linux"},{"name":"extra"}`, 1), githubsets.ErrRegistrationDrift},
		{"unexpected pinned ID", strings.Replace(matchingSetResponse, `"id":7`, `"id":8`, 1), githubsets.ErrRegistrationDrift},
		{"different group", strings.Replace(matchingSetResponse, `"runnerGroupId":19`, `"runnerGroupId":20`, 1), githubsets.ErrRegistrationDrift},
	} {
		t.Run(tt.name, func(t *testing.T) {
			github := newScript(t,
				registrationExchange(t, "api.github.com", "/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
				adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
				groupLookup(t), setLookup(t, tt.response),
			)
			loader := credentialLoader(newScript(t, secretExchange(t, "v1", `{"token":"fixture-pat"}`)), github)
			connection, err := loader.Load(t.Context(), cfg, "build")
			if err != nil {
				t.Fatal(err)
			}
			if err := connection.Validate(t.Context()); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestGitHubSecretErrorsDoNotRevealPayloads(t *testing.T) {
	cfg := credentialArtifact("enterprise-pat", "https://github.com/enterprises/corp")
	for _, document := range []string{
		`{"token":"fixture-secret","token":"fixture-secret"}`,
		`{"token":"fixture-secret","foo":"fixture-secret"}`,
		`{"token":"fixture-secret"} {"fixture-secret":true}`,
	} {
		loader := credentialLoader(newScript(t, secretExchange(t, "v1", document)), newScript(t))
		_, err := loader.Load(t.Context(), cfg, "build")
		if !errors.Is(err, githubsets.ErrInvalidSecret) || strings.Contains(err.Error(), "fixture-secret") {
			t.Fatalf("expected redacted invalid secret error, got %v", err)
		}
	}
}

func TestGitHubSecretReadErrorsDoNotRevealProviderResponse(t *testing.T) {
	cfg := credentialArtifact("enterprise-pat", "https://github.com/enterprises/corp")
	secrets := newScript(t, exchange{method: "POST", path: "/", status: http.StatusBadRequest,
		response: `{"__type":"AccessDeniedException","message":"fixture-secret"}`})
	loader := credentialLoader(secrets, newScript(t))
	_, err := loader.Load(context.Background(), cfg, "build")
	if !errors.Is(err, githubsets.ErrSecretUnavailable) || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("expected redacted provider error, got %v", err)
	}
}
