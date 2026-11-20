// Package githubsets adapts the GitHub Scale Set SDK to configured runner profiles.
package githubsets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"unicode"

	"github.com/actions/scaleset"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

var (
	ErrInactive          = errors.New("GitHub credentials require active mode")
	ErrUnusedConnection  = errors.New("GitHub connection has no enabled profiles")
	ErrSecretUnavailable = errors.New("could not read GitHub credential from Secrets Manager")
	ErrInvalidSecret     = errors.New("invalid GitHub credential secret")
)

type SecretsManager interface {
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type Loader struct {
	Secrets SecretsManager
	// WrapTransport optionally instruments the credential client's verified TLS transport.
	WrapTransport func(http.RoundTripper) http.RoundTripper
}

// Load reads AWSCURRENT for one selected connection. Callers pass a validated
// artifact and use Validate before replacing an existing connection.
func (l Loader) Load(ctx context.Context, artifact runtimeconfig.Artifact, name string) (*Connection, error) {
	spec := artifact.Config.Spec
	if spec.Mode != "active" {
		return nil, ErrInactive
	}
	var profiles []runtimeconfig.Profile
	for _, profile := range spec.Profiles {
		if profile.State != "disabled" && profile.GitHub.Connection == name {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		return nil, ErrUnusedConnection
	}
	config, ok := spec.GitHub[name]
	if !ok {
		return nil, errors.New("missing configured GitHub connection")
	}
	scope, err := config.CanonicalScope()
	if err != nil {
		return nil, err
	}
	httpClient, err := githubHTTPClient(config.CABundlePath, l.WrapTransport)
	if err != nil {
		return nil, err
	}
	client, version, err := l.loadClient(ctx, config.Auth, scope, httpClient)
	if err != nil {
		return nil, err
	}
	return &Connection{
		client: client, deployment: artifact.Config.Metadata.Name,
		scope: scope, profiles: profiles, VersionID: version,
	}, nil
}

func (l Loader) loadClient(ctx context.Context, auth runtimeconfig.GitHubAuth, scope string, httpClient *http.Client) (*scaleset.Client, string, error) {
	if l.Secrets == nil || auth.SecretARN == "" {
		return nil, "", ErrSecretUnavailable
	}
	secret, err := l.Secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(auth.SecretARN), VersionStage: aws.String("AWSCURRENT"),
	}, func(options *secretsmanager.Options) {
		options.ClientLogMode = 0
	})
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return nil, "", context.Canceled
		case errors.Is(err, context.DeadlineExceeded):
			return nil, "", context.DeadlineExceeded
		default:
			return nil, "", ErrSecretUnavailable
		}
	}
	if secret == nil || aws.ToString(secret.ARN) != auth.SecretARN || aws.ToString(secret.VersionId) == "" ||
		secret.SecretString == nil || len(secret.SecretBinary) != 0 {
		return nil, "", ErrInvalidSecret
	}
	fields, err := secretFields(*secret.SecretString)
	if err != nil {
		return nil, "", err
	}
	client, err := newClient(scope, auth.Type, fields, httpClient)
	if err != nil {
		return nil, "", err
	}
	return client, *secret.VersionId, nil
}

func newClient(scope, kind string, fields map[string]json.RawMessage, httpClient *http.Client) (*scaleset.Client, error) {
	retry := scaleset.DefaultRetryConfig()
	retry.OnRateLimit = scaleset.RateLimitRetries(scaleset.RateLimitRule{}, scaleset.RateLimitRule{})
	options := []scaleset.HTTPOption{
		scaleset.WithHTTPClient(httpClient),
		scaleset.WithLogger(slog.New(slog.DiscardHandler)),
		scaleset.WithRetry(retry),
	}
	var client *scaleset.Client
	var err error
	switch kind {
	case "enterprise-pat":
		var token string
		if len(fields) != 1 || json.Unmarshal(fields["token"], &token) != nil ||
			token == "" || strings.IndexFunc(token, unicode.IsSpace) >= 0 {
			return nil, fmt.Errorf("%w: expected one nonempty token field", ErrInvalidSecret)
		}
		client, err = scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
			GitHubConfigURL: scope, PersonalAccessToken: token,
		}, options...)
	case "github-app":
		var auth scaleset.GitHubAppAuth
		if len(fields) != 3 || json.Unmarshal(fields["clientId"], &auth.ClientID) != nil ||
			json.Unmarshal(fields["installationId"], &auth.InstallationID) != nil ||
			json.Unmarshal(fields["privateKey"], &auth.PrivateKey) != nil ||
			strings.TrimSpace(auth.ClientID) == "" || auth.InstallationID <= 0 || auth.PrivateKey == "" {
			return nil, fmt.Errorf("%w: expected clientId, installationId and privateKey", ErrInvalidSecret)
		}
		client, err = scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
			GitHubConfigURL: scope, GitHubAppAuth: auth,
		}, options...)
	default:
		return nil, errors.New("unsupported GitHub credential type")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: could not initialise GitHub authentication", ErrInvalidSecret)
	}
	return client, nil
}

func secretFields(value string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, ErrInvalidSecret
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, ErrInvalidSecret
		}
		if _, exists := fields[key]; exists {
			return nil, ErrInvalidSecret
		}
		var field json.RawMessage
		if err := decoder.Decode(&field); err != nil {
			return nil, ErrInvalidSecret
		}
		fields[key] = field
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrInvalidSecret
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, ErrInvalidSecret
	}
	return fields, nil
}
