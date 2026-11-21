package githubsets

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	client, err := newClient("https://github.com/enterprises/fixture", "enterprise-pat",
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
