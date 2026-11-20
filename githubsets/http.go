package githubsets

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
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

type responseStatusKey struct{}

func trackResponseStatus(ctx context.Context) (context.Context, *atomic.Int32) {
	status := &atomic.Int32{}
	return context.WithValue(ctx, responseStatusKey{}, status), status
}

type githubTransport struct {
	next http.RoundTripper
}

func (t githubTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" || request.URL.User != nil {
		return nil, errors.New("GitHub requests require HTTPS without URL credentials")
	}
	response, err := t.next.RoundTrip(request)
	if status, ok := request.Context().Value(responseStatusKey{}).(*atomic.Int32); ok {
		status.Store(0)
		if response != nil {
			status.Store(int32(response.StatusCode))
		}
	}
	return response, err
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

func requestError(operation string, status int32, err error) error {
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
