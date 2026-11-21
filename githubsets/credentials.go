// Package githubsets adapts the GitHub Scale Set SDK to configured runner profiles.
package githubsets

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
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
	credential, err := l.loadClient(ctx, config.Auth, scope, httpClient)
	if err != nil {
		return nil, err
	}
	return &Connection{
		client: credential.client, deployment: artifact.Config.Metadata.Name,
		scope: scope, profiles: profiles, VersionID: credential.version, rateLimitKey: credential.limitKey,
	}, nil
}

type loadedCredential struct {
	client   *scaleset.Client
	version  string
	limitKey [32]byte
}

func (l Loader) loadClient(ctx context.Context, auth runtimeconfig.GitHubAuth, scope string, httpClient *http.Client) (loadedCredential, error) {
	if l.Secrets == nil || auth.SecretARN == "" {
		return loadedCredential{}, ErrSecretUnavailable
	}
	secret, err := l.Secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(auth.SecretARN), VersionStage: aws.String("AWSCURRENT"),
	}, func(options *secretsmanager.Options) {
		options.ClientLogMode = 0
	})
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return loadedCredential{}, context.Canceled
		case errors.Is(err, context.DeadlineExceeded):
			return loadedCredential{}, context.DeadlineExceeded
		default:
			return loadedCredential{}, ErrSecretUnavailable
		}
	}
	if secret == nil || aws.ToString(secret.ARN) != auth.SecretARN || aws.ToString(secret.VersionId) == "" ||
		secret.SecretString == nil || len(secret.SecretBinary) != 0 {
		return loadedCredential{}, ErrInvalidSecret
	}
	fields, err := secretFields(*secret.SecretString)
	if err != nil {
		return loadedCredential{}, err
	}
	client, limitKey, err := newClient(scope, auth.Type, fields, httpClient)
	if err != nil {
		return loadedCredential{}, err
	}
	return loadedCredential{client: client, version: *secret.VersionId, limitKey: limitKey}, nil
}

func newClient(scope, kind string, fields map[string]json.RawMessage, httpClient *http.Client) (*scaleset.Client, [32]byte, error) {
	retry := scaleset.DefaultRetryConfig()
	retry.OnRateLimit = scaleset.RateLimitRetries(scaleset.RateLimitRule{}, scaleset.RateLimitRule{})
	options := []scaleset.HTTPOption{
		scaleset.WithHTTPClient(httpClient),
		scaleset.WithLogger(slog.New(slog.DiscardHandler)),
		scaleset.WithRetry(retry),
	}
	info := scaleset.SystemInfo{System: "scaleset-example-aws", Subsystem: "controller"}
	var client *scaleset.Client
	var err error
	var identity string
	switch kind {
	case "enterprise-pat":
		var token string
		if len(fields) != 1 || json.Unmarshal(fields["token"], &token) != nil ||
			token == "" || strings.IndexFunc(token, unicode.IsSpace) >= 0 {
			return nil, [32]byte{}, fmt.Errorf("%w: expected one nonempty token field", ErrInvalidSecret)
		}
		identity = token
		client, err = scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
			GitHubConfigURL: scope, PersonalAccessToken: token, SystemInfo: info,
		}, options...)
	case "github-app":
		var auth scaleset.GitHubAppAuth
		if len(fields) != 3 || json.Unmarshal(fields["clientId"], &auth.ClientID) != nil ||
			json.Unmarshal(fields["installationId"], &auth.InstallationID) != nil ||
			json.Unmarshal(fields["privateKey"], &auth.PrivateKey) != nil ||
			strings.TrimSpace(auth.ClientID) == "" || auth.InstallationID <= 0 || auth.PrivateKey == "" {
			return nil, [32]byte{}, fmt.Errorf("%w: expected clientId, installationId and privateKey", ErrInvalidSecret)
		}
		identity = strconv.FormatInt(auth.InstallationID, 10)
		client, err = scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
			GitHubConfigURL: scope, GitHubAppAuth: auth, SystemInfo: info,
		}, options...)
	default:
		return nil, [32]byte{}, errors.New("unsupported GitHub credential type")
	}
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("%w: could not initialise GitHub authentication", ErrInvalidSecret)
	}
	host, _, _ := strings.Cut(strings.TrimPrefix(scope, "https://"), "/")
	return client, sha256.Sum256([]byte(host + "\x00" + kind + "\x00" + identity)), nil
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
