package githubsets

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
)

var (
	ErrUnauthorized = errors.New("GitHub rejected the configured credential or its permissions")
	ErrRequest      = errors.New("GitHub request failed")
	ErrRateLimited  = errors.New("GitHub rate limited the request")
)

type RateLimitError struct {
	RetryAfter time.Duration
	Reset      time.Time
}

func (e *RateLimitError) Error() string { return ErrRateLimited.Error() }
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

type credentialRetryKey struct{}

func oneWriteAttempt(ctx context.Context) context.Context {
	credentialRequest := &atomic.Bool{}
	ctx = context.WithValue(ctx, credentialRetryKey{}, credentialRequest)
	return scaleset.WithRequestRetry(ctx, func(retry *scaleset.RetryConfig) {
		shouldRetry := retry.ShouldRetry
		if shouldRetry == nil {
			shouldRetry = scaleset.DefaultShouldRetry
		}
		onRateLimit := retry.OnRateLimit
		if onRateLimit == nil {
			onRateLimit = scaleset.DefaultOnRateLimit
		}
		retry.ShouldRetry = func(ctx context.Context, response *http.Response, err error) (bool, error) {
			if !credentialRequest.Load() {
				return false, nil
			}
			return shouldRetry(ctx, response, err)
		}
		retry.OnRateLimit = func(ctx context.Context, limit *scaleset.RateLimitError) (time.Duration, bool) {
			if !credentialRequest.Load() {
				return 0, false
			}
			return onRateLimit(ctx, limit)
		}
	})
}

type githubTransport struct {
	next http.RoundTripper
}

func (t githubTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" || request.URL.User != nil {
		return nil, errors.New("GitHub requests require HTTPS without URL credentials")
	}
	if credentialRequest, ok := request.Context().Value(credentialRetryKey{}).(*atomic.Bool); ok {
		credentialRequest.Store(isCredentialExchange(request))
	}
	return t.next.RoundTrip(request)
}

// The SDK can exchange credentials while building a mutation. Keep normal
// retries for those exchanges, and default all unrecognized requests to one attempt.
func isCredentialExchange(request *http.Request) bool {
	path := request.URL.Path
	if request.Method == http.MethodPatch {
		return strings.Contains(path, "/_apis/runtime/runnerscalesets/") && strings.Contains(path, "/sessions/")
	}
	if request.Method != http.MethodPost {
		return false
	}
	return strings.HasSuffix(path, "/actions/runner-registration") ||
		strings.HasSuffix(path, "/actions/runners/registration-token") ||
		(strings.Contains(path, "/app/installations/") && strings.HasSuffix(path, "/access_tokens"))
}

func githubHTTPClient(caBundle string, wrap func(http.RoundTripper) http.RoundTripper) (*http.Client, error) {
	config := scaleset.HTTPClientConfig{}
	if caBundle != "" {
		pem, err := os.ReadFile(caBundle)
		if err != nil {
			return nil, errors.New("could not read GitHub CA bundle")
		}
		roots, err := x509.SystemCertPool()
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("could not load GitHub CA bundle")
		}
		config.RootCAs = roots
	}
	client := scaleset.NewHTTPClient(config)
	transport := client.Transport
	if wrap != nil {
		transport = wrap(transport)
		if transport == nil {
			return nil, errors.New("GitHub transport wrapper returned no transport")
		}
	}
	client.Transport = githubTransport{next: transport}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client, nil
}

func requestError(operation string, err error) error {
	var status int
	if response, ok := errors.AsType[*scaleset.RequestResponseError](err); ok && response.Response != nil {
		status = response.Response.StatusCode
	}
	// SDK errors can include response bodies, headers or credential-bearing URLs.
	var reason error
	switch limit, ok := errors.AsType[*scaleset.RateLimitError](err); {
	case ok:
		reason = &RateLimitError{RetryAfter: limit.RetryAfter, Reset: limit.Reset}
	case status == http.StatusUnauthorized || status == http.StatusForbidden || errors.Is(err, scaleset.UnauthorizedError):
		reason = ErrUnauthorized
	case errors.Is(err, context.Canceled):
		reason = context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		reason = context.DeadlineExceeded
	default:
		reason = ErrRequest
	}
	return fmt.Errorf("%s: %w", operation, reason)
}
