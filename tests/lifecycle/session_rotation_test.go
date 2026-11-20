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
	"github.com/knightfall/scaleset-example-aws/listener"
)

type rotatingSecret string

func (version rotatingSecret) GetSecretValue(_ context.Context, input *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return &secretsmanager.GetSecretValueOutput{
		ARN: input.SecretId, SecretString: aws.String(fmt.Sprintf(`{"token":%q}`, string(version))), VersionId: aws.String(string(version)),
	}, nil
}

type rotationFixture struct {
	t          *testing.T
	ledger     *lifecycle.Store
	openCalls  int
	closeCalls int
	active     string
	loseOpen   bool
	loseClose  bool
}

func (f *rotationFixture) RoundTrip(request *http.Request) (*http.Response, error) {
	f.t.Helper()
	if request.Body != nil {
		defer func() {
			if err := request.Body.Close(); err != nil {
				f.t.Errorf("close fixture request body: %v", err)
			}
		}()
	}
	status := http.StatusOK
	var body string
	switch {
	case request.URL.Path == "/enterprises/fixture/actions/runners/registration-token":
		status = http.StatusCreated
		body = `{"token":"registration-fixture","expires_at":"2099-01-01T00:00:00Z"}`
		if request.Header.Get("Authorization") == "Bearer denied" {
			status = http.StatusForbidden
			body = `{"message":"fixture-private-rejection"}`
		}
	case request.URL.Path == "/actions/runner-registration":
		payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
		token := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fixture"
		body = fmt.Sprintf(`{"url":"https://actions.example.invalid","token":%q}`, token)
	case request.URL.Path == "/_apis/runtime/runnergroups/":
		body = `{"count":1,"value":[{"id":19,"name":"Build"}]}`
	case request.URL.Path == "/_apis/runtime/runnerscalesets":
		body = `{"count":1,"value":[{"id":7,"name":"linux","runnerGroupId":19,"labels":[{"name":"linux"}]}]}`
	case request.Method == http.MethodPost && request.URL.Path == "/_apis/runtime/runnerscalesets/7/sessions":
		return f.open(request)
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/_apis/runtime/runnerscalesets/7/sessions/"):
		return f.close(request)
	default:
		f.t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
	}
	return fixtureReply(status, body), nil
}

func (f *rotationFixture) open(request *http.Request) (*http.Response, error) {
	f.t.Helper()
	var payload struct{ OwnerName string }
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		f.t.Fatal(err)
	}
	owned, err := f.ledger.SessionOwnership(request.Context(), "dev", "build")
	if err != nil || owned.Phase != lifecycle.SessionOpening || owned.OwnerName != payload.OwnerName || f.active != "" {
		f.t.Fatalf("POST without exclusive open intent: %+v, error = %v, active = %q", owned, err, f.active)
	}
	f.openCalls++
	f.active = fmt.Sprintf("00000000-0000-0000-0000-%012d", f.openCalls)
	if f.loseOpen {
		return nil, errors.New("fixture-private-open")
	}
	body := fmt.Sprintf(`{"sessionId":%q,"ownerName":%q,"runnerScaleSet":{"id":7},
		"messageQueueUrl":"https://queue.example.invalid/messages","messageQueueAccessToken":"fixture-token",
		"statistics":{"totalAssignedJobs":1}}`, f.active, payload.OwnerName)
	return fixtureReply(http.StatusOK, body), nil
}

func (f *rotationFixture) close(request *http.Request) (*http.Response, error) {
	f.t.Helper()
	owned, err := f.ledger.SessionOwnership(request.Context(), "dev", "build")
	if err != nil || owned.Phase != lifecycle.SessionClosing || owned.SessionID != f.active ||
		!strings.HasSuffix(request.URL.Path, "/"+f.active) {
		f.t.Fatalf("DELETE without scoped close intent: %+v, error = %v, active = %q", owned, err, f.active)
	}
	f.closeCalls++
	f.active = ""
	if f.loseClose {
		return nil, errors.New("fixture-private-close")
	}
	return fixtureReply(http.StatusNoContent, ""), nil
}

func fixtureReply(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func (f *rotationFixture) connection(version string) *githubsets.Connection {
	f.t.Helper()
	loader := githubsets.Loader{
		Secrets: rotatingSecret(version),
		WrapTransport: func(http.RoundTripper) http.RoundTripper {
			return f
		},
	}
	connection, err := loader.Load(f.t.Context(), githubCreationConfig(), "build")
	if err != nil {
		f.t.Fatal(err)
	}
	return connection
}

func TestCredentialHandoverConfirmsCloseBeforeOpeningReplacement(t *testing.T) {
	ctx := t.Context()
	ledger, _ := store(t)
	lease, err := ledger.AcquireLease(ctx, "dev", "controller", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	wire := rotationFixture{t: t, ledger: ledger}
	first, err := wire.connection("current").OpenOwnedSession(ctx, ledger, lease, "build")
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := wire.connection("current").ReplaceOwnedSession(ctx, ledger, lease, first)
	if err != nil || unchanged != first || wire.openCalls != 1 || wire.closeCalls != 0 {
		t.Fatalf("same credential version changed the session: %v", err)
	}
	if _, err := wire.connection("denied").ReplaceOwnedSession(ctx, ledger, lease, first); !errors.Is(err, githubsets.ErrUnauthorized) ||
		strings.Contains(err.Error(), "fixture-private") || wire.active != first.ID() || wire.closeCalls != 0 {
		t.Fatalf("invalid candidate did not preserve current session: %v", err)
	}
	next, err := wire.connection("candidate").ReplaceOwnedSession(ctx, ledger, lease, first)
	if err != nil || next.ID() != wire.active || next.ID() == first.ID() || wire.openCalls != 2 || wire.closeCalls != 1 {
		t.Fatalf("handover active session = %q, POST = %d, DELETE = %d, error = %v", wire.active, wire.openCalls, wire.closeCalls, err)
	}
	progress, err := ledger.SessionProgress(ctx, "dev", "build")
	if err != nil || progress.SessionID != next.ID() || progress.LastAcknowledged != 0 || progress.Demand == nil || progress.Demand.Assigned != 1 {
		t.Fatalf("replacement cursor = %+v, error = %v", progress, err)
	}
	old := listener.Processor{Store: ledger, Session: first, MaxDemandAge: time.Minute}
	if err := old.Step(ctx, lease, "build", nil); !errors.Is(err, lifecycle.ErrSessionChanged) {
		t.Fatalf("superseded processor returned %v", err)
	}
	if err := next.CloseOwned(ctx, ledger, lease); err != nil {
		t.Fatal(err)
	}
	if err := next.CloseOwned(ctx, ledger, lease); err != nil || wire.closeCalls != 2 {
		t.Fatalf("duplicate close was not idempotent: %v, DELETEs = %d", err, wire.closeCalls)
	}
}

func TestAmbiguousSessionMutationsBlockAnotherWireAttempt(t *testing.T) {
	for _, failure := range []string{"open", "close"} {
		t.Run(failure, func(t *testing.T) {
			ctx := t.Context()
			ledger, _ := store(t)
			lease, err := ledger.AcquireLease(ctx, "dev", "controller", time.Now(), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			wire := rotationFixture{t: t, ledger: ledger, loseOpen: failure == "open", loseClose: failure == "close"}
			connection := wire.connection("current")
			session, err := connection.OpenOwnedSession(ctx, ledger, lease, "build")
			wantClose := 0
			if failure == "close" {
				if err != nil {
					t.Fatal(err)
				}
				err = session.CloseOwned(ctx, ledger, lease)
				wantClose = 1
			}
			if !errors.Is(err, githubsets.ErrSessionUnconfirmed) || strings.Contains(err.Error(), "fixture-private") {
				t.Fatalf("ambiguous %s error = %v", failure, err)
			}
			if _, err := connection.OpenOwnedSession(ctx, ledger, lease, "build"); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
				t.Fatalf("ambiguous %s allowed replacement: %v", failure, err)
			}
			if failure == "close" {
				if err := session.CloseOwned(ctx, ledger, lease); !errors.Is(err, lifecycle.ErrSessionRecoveryRequired) {
					t.Fatalf("ambiguous close was attempted again: %v", err)
				}
			}
			if wire.openCalls != 1 || wire.closeCalls != wantClose {
				t.Fatalf("wire attempts after ambiguity = %d POST, %d DELETE", wire.openCalls, wire.closeCalls)
			}
		})
	}
}
