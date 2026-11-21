package githubsets

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOneWriteAttemptKeepsCredentialExchangeRetries(t *testing.T) {
	var exchanges, creates int
	mutationFailed := errors.New("fixture lost create response")
	transport := transportFunc(func(req *http.Request) (*http.Response, error) {
		if req.Body != nil {
			if err := req.Body.Close(); err != nil {
				return nil, err
			}
		}
		status := http.StatusOK
		var body string
		switch req.URL.Path {
		case "/enterprises/fixture/actions/runners/registration-token":
			status = http.StatusCreated
			body = `{"token":"registration-fixture","expires_at":"2099-01-01T00:00:00Z"}`
		case "/actions/runner-registration":
			exchanges++
			if exchanges == 1 {
				status, body = http.StatusUnauthorized, "{}"
			} else {
				payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
				token := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fixture"
				body = fmt.Sprintf(`{"url":"https://actions.example.invalid","token":%q}`, token)
			}
		case "/_apis/runtime/runnerscalesets":
			creates++
			return nil, mutationFailed
		default:
			t.Errorf("unexpected fixture request: %s %s", req.Method, req.URL.Path)
			return nil, errors.New("unexpected fixture request")
		}
		return &http.Response{
			StatusCode: status, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), Request: req,
		}, nil
	})
	httpClient, err := githubHTTPClient("", func(http.RoundTripper) http.RoundTripper { return transport })
	if err != nil {
		t.Fatal(err)
	}
	client, _, err := newClient("https://github.com/enterprises/fixture", "enterprise-pat",
		map[string]json.RawMessage{"token": json.RawMessage(`"fixture-pat"`)}, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx := oneWriteAttempt(t.Context())
	_, err = client.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{Name: "fixture"})
	if !errors.Is(err, mutationFailed) || exchanges != 2 || creates != 1 {
		t.Fatalf("error = %v, credential exchanges = %d, create attempts = %d", err, exchanges, creates)
	}
}

func TestCredentialRateLimitIdentityIsIndependentOfConnectionScope(t *testing.T) {
	client, err := githubHTTPClient("", nil)
	if err != nil {
		t.Fatal(err)
	}
	key := func(t *testing.T, scope, token string) [32]byte {
		t.Helper()
		_, key, err := newClient(scope, "enterprise-pat",
			map[string]json.RawMessage{"token": json.RawMessage(fmt.Sprintf("%q", token))}, client)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	original := key(t, "https://github.com/enterprises/first", "fixture-pat")
	if original == ([32]byte{}) || original != key(t, "https://github.com/enterprises/second", "fixture-pat") {
		t.Fatal("equivalent credentials do not share a rate-limit identity")
	}
	if original == key(t, "https://customer.ghe.com/enterprises/first", "fixture-pat") ||
		original == key(t, "https://github.com/enterprises/first", "another-pat") {
		t.Fatal("unrelated credentials share a rate-limit identity")
	}
}

func TestRequestErrorPreservesTheUsedCredentialRateLimitKey(t *testing.T) {
	key := [32]byte{1}
	err := requestError("poll", &scaleset.RateLimitError{RetryAfter: time.Second}, key)
	limit, ok := errors.AsType[*RateLimitError](err)
	if !ok || limit.CredentialKey != key || limit.RetryAfter != time.Second {
		t.Fatalf("rate limit translation = %v", err)
	}
}

func TestNotFoundErrorsAreDistinctAndSanitized(t *testing.T) {
	for _, original := range []error{scaleset.NotFoundError, &scaleset.RequestResponseError{
		Response: &http.Response{StatusCode: http.StatusNotFound},
	}} {
		err := requestError("lookup", original, [32]byte{})
		if !errors.Is(err, ErrNotFound) || err.Error() != "lookup: "+ErrNotFound.Error() {
			t.Fatalf("absence translation: %v", err)
		}
	}
}

func TestRejectedTransportRequestClearsCredentialRetryAttribution(t *testing.T) {
	ctx := oneWriteAttempt(t.Context())
	attribution := ctx.Value(credentialRetryKey{}).(*atomic.Bool)
	attribution.Store(true)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://example.invalid/mutation", nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := githubTransport{next: transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid request reached the wire")
		return nil, nil
	})}
	if _, err := transport.RoundTrip(request); err == nil || attribution.Load() {
		t.Fatalf("invalid mutation retained credential attribution: %v", err)
	}
}
