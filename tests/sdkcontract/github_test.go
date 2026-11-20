package sdkcontract

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

const (
	scaleSetPath = "/_apis/runtime/runnerscalesets"
	sessionID    = "ddc42a1b-0b63-4dec-acc6-d92bc0d7135f"
	sessionPath  = scaleSetPath + "/7/sessions"
	queuePath    = "/messages"
)

func registrationExchange(t *testing.T, host, path, auth string) exchange {
	t.Helper()
	return exchange{
		method: "POST", path: path, status: 201,
		response: `{"token":"registration-fixture","expires_at":"2099-01-01T00:00:00Z"}`,
		check: func(r *http.Request) {
			checkValue(t, "registration host", r.URL.Host, host)
			checkValue(t, "registration authorization", r.Header.Get("Authorization"), auth)
		},
	}
}

func adminExchange(t *testing.T, host, path, scope string) exchange {
	t.Helper()
	payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
	token := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fixture"
	return exchange{
		method: "POST", path: path, status: 200,
		response: fmt.Sprintf(`{"url":"https://actions.example.invalid","token":%q}`, token),
		check: func(r *http.Request) {
			checkValue(t, "admin host", r.URL.Host, host)
			checkValue(t, "admin exchange authorization", r.Header.Get("Authorization"), "RemoteAuth registration-fixture")
			checkJSON(t, r, fmt.Sprintf(`{"url":%q,"runner_event":"register"}`, scope))
		},
	}
}

func newPAT(t *testing.T, scope string, wire *scriptedHTTP) *scaleset.Client {
	t.Helper()
	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL: scope, PersonalAccessToken: "fixture-pat",
	}, scaleset.WithHTTPClient(wire))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestEnterprisePATScope(t *testing.T) {
	for _, tc := range []struct {
		name    string
		host    string
		apiHost string
		prefix  string
	}{
		{name: "GitHub Cloud", host: "github.com", apiHost: "api.github.com"},
		{name: "GHE Cloud", host: "corp.ghe.com", apiHost: "api.corp.ghe.com"},
		{name: "GHES", host: "git.example.invalid", apiHost: "git.example.invalid", prefix: "/api/v3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := "https://" + tc.host + "/enterprises/corp"
			wire := newScript(t,
				registrationExchange(t, tc.apiHost, tc.prefix+"/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
				adminExchange(t, tc.apiHost, tc.prefix+"/actions/runner-registration", scope),
				exchange{method: "GET", path: "/_apis/runtime/runnergroups/", status: 200,
					response: `{"count":1,"value":[{"id":19,"name":"Build Group"}]}`,
					check: func(r *http.Request) {
						checkValue(t, "groupName", r.URL.Query().Get("groupName"), "Build Group")
					}},
				exchange{method: "GET", path: scaleSetPath, status: 200,
					response: `{"count":1,"value":[{"id":7,"name":"linux-x64","runnerGroupId":19,"labels":[{"name":"linux-x64","type":"System"}]}]}`,
					check: func(r *http.Request) {
						checkValue(t, "group ID", r.URL.Query().Get("runnerGroupId"), "19")
						checkValue(t, "set name", r.URL.Query().Get("name"), "linux-x64")
					}},
			)
			client := newPAT(t, scope, wire)
			group, err := client.GetRunnerGroupByName(t.Context(), "Build Group")
			if err != nil || group == nil || group.ID != 19 {
				t.Fatalf("group = %+v, error = %v", group, err)
			}
			set, err := client.GetRunnerScaleSet(t.Context(), group.ID, "linux-x64")
			if err != nil || set == nil || set.ID != 7 || set.RunnerGroupID != 19 ||
				len(set.Labels) != 1 || set.Labels[0].Name != "linux-x64" {
				t.Fatalf("scale set = %+v, error = %v", set, err)
			}
		})
	}
}

func TestGitHubAppScopeAndScaleSetLabels(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	const scope = "https://github.com/example/build"
	wire := newScript(t,
		exchange{method: "POST", path: "/app/installations/42/access_tokens", status: 201,
			response: `{"token":"installation-fixture","expires_at":"2099-01-01T00:00:00Z"}`,
			check: func(r *http.Request) {
				checkValue(t, "App host", r.URL.Host, "api.github.com")
				if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "Bearer ") || strings.Count(auth, ".") != 2 {
					t.Error("app exchange did not send a bearer JWT")
				}
			}},
		registrationExchange(t, "api.github.com", "/repos/example/build/actions/runners/registration-token", "Bearer installation-fixture"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		exchange{method: "POST", path: scaleSetPath, status: 200,
			response: `{"id":7,"name":"linux-x64","runnerGroupId":19,"labels":[{"name":"linux-x64","type":"System"},{"name":"docker-linux","type":"System"}]}`,
			check: func(r *http.Request) {
				checkJSON(t, r, `{
					"name":"linux-x64", "runnerGroupId":19, "labels":[
						{"name":"linux-x64","type":"System"},{"name":"docker-linux","type":"System"}
					], "RunnerSetting":{"disableUpdate":true},"createdOn":"0001-01-01T00:00:00Z"
				}`)
			}},
	)
	client, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: scope,
		GitHubAppAuth:   scaleset.GitHubAppAuth{ClientID: "Iv1.fixture", InstallationID: 42, PrivateKey: string(keyPEM)},
	}, scaleset.WithHTTPClient(wire))
	if err != nil {
		t.Fatal(err)
	}
	set, err := client.CreateRunnerScaleSet(t.Context(), &scaleset.RunnerScaleSet{
		Name: "linux-x64", RunnerGroupID: 19,
		Labels:        []scaleset.Label{{Name: "linux-x64"}, {Name: "docker-linux"}},
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	})
	if err != nil || set == nil || set.ID != 7 || len(set.Labels) != 2 {
		t.Fatalf("scale set = %+v, error = %v", set, err)
	}
}

func TestGitHubSessionWire(t *testing.T) {
	const scope = "https://github.com/enterprises/corp"
	sessionResponse := func(token string) string {
		return fmt.Sprintf(`{"sessionId":%q,"ownerName":"fixture-owner","runnerScaleSet":{"id":7},"messageQueueUrl":"https://queue.example.invalid/messages","messageQueueAccessToken":%q,"statistics":{"totalAssignedJobs":1}}`, sessionID, token)
	}
	checkPoll := func(token, cursor, capacity string) func(*http.Request) {
		return func(r *http.Request) {
			checkValue(t, "queue host", r.URL.Host, "queue.example.invalid")
			checkValue(t, "queue authorization", r.Header.Get("Authorization"), "Bearer "+token)
			checkValue(t, "max capacity", r.Header.Get(scaleset.HeaderScaleSetMaxCapacity), capacity)
			checkValue(t, "last message ID", r.URL.Query().Get("lastMessageId"), cursor)
		}
	}
	wire := newScript(t,
		registrationExchange(t, "api.github.com", "/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		exchange{method: "POST", path: sessionPath, status: 200, response: sessionResponse("queue-first"),
			check: func(r *http.Request) {
				checkJSON(t, r, `{"sessionId":"00000000-0000-0000-0000-000000000000","ownerName":"fixture-owner"}`)
			}},
		exchange{method: "GET", path: queuePath, status: 401, check: checkPoll("queue-first", "18", "4")},
		exchange{method: "PATCH", path: sessionPath + "/" + sessionID, status: 200, response: sessionResponse("queue-current")},
		exchange{method: "GET", path: queuePath, status: 200, check: checkPoll("queue-current", "18", "4"),
			response: `{"messageId":19,"messageType":"RunnerScaleSetJobMessages",
				"body":"[{\"messageType\":\"JobAvailable\",\"runnerRequestId\":101},{\"messageType\":\"JobAssigned\",\"runnerRequestId\":100}]",
				"statistics":{"totalAssignedJobs":2,"totalRunningJobs":1}}`},
		exchange{method: "POST", path: scaleSetPath + "/7/acquirejobs", status: 200,
			response: `{"count":1,"value":[101]}`,
			check: func(r *http.Request) {
				checkValue(t, "acquire authorization", r.Header.Get("Authorization"), "Bearer queue-current")
				checkJSON(t, r, `[101,102]`)
			}},
		exchange{method: "DELETE", path: queuePath + "/19", status: 204,
			check: func(r *http.Request) {
				checkValue(t, "ack authorization", r.Header.Get("Authorization"), "Bearer queue-current")
			}},
		exchange{method: "GET", path: queuePath, status: 202, check: checkPoll("queue-current", "19", "0")},
		exchange{method: "DELETE", path: sessionPath + "/" + sessionID, status: 204},
	)

	session, err := newPAT(t, scope, wire).MessageSessionClient(t.Context(), 7, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	if stats := session.Session().Statistics; stats == nil || stats.TotalAssignedJobs != 1 {
		t.Fatalf("initial statistics = %+v", stats)
	}
	message, err := session.GetMessage(t.Context(), 18, 4)
	if err != nil || message == nil || message.MessageID != 19 || message.Statistics == nil ||
		message.Statistics.TotalAssignedJobs != 2 || message.Statistics.TotalRunningJobs != 1 ||
		len(message.JobAvailableMessages) != 1 || message.JobAvailableMessages[0].RunnerRequestID != 101 ||
		len(message.JobAssignedMessages) != 1 || message.JobAssignedMessages[0].RunnerRequestID != 100 {
		t.Fatalf("message = %+v, error = %v", message, err)
	}
	ids, err := session.AcquireJobs(t.Context(), []int64{101, 102})
	if err != nil || !slices.Equal(ids, []int64{101}) {
		t.Fatalf("acquired = %v, error = %v", ids, err)
	}
	if err := session.DeleteMessage(t.Context(), 19); err != nil {
		t.Fatal(err)
	}
	if message, err := session.GetMessage(t.Context(), 19, 0); message != nil || err != nil {
		t.Fatalf("idle poll = %+v, error = %v", message, err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubJITLostResponseUsesOneAttempt(t *testing.T) {
	const scope = "https://github.com/enterprises/corp"
	lostResponse := errors.New("fixture: lost response")
	const request = `{"name":"linux-x64-0001","workFolder":"_work"}`
	check := func(r *http.Request) { checkJSON(t, r, request) }
	wire := newScript(t,
		registrationExchange(t, "api.github.com", "/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		exchange{method: "POST", path: scaleSetPath + "/7/generatejitconfig", status: 200,
			response: `{"runner":{"id":12,"name":"linux-x64-0000","runnerScaleSetId":7},"encodedJITConfig":"fixture-config"}`,
			check:    func(r *http.Request) { checkJSON(t, r, `{"name":"linux-x64-0000","workFolder":"_work"}`) }},
		exchange{method: "POST", path: scaleSetPath + "/7/generatejitconfig", err: lostResponse, check: check},
		exchange{method: "GET", path: "/_apis/distributedtask/pools/0/agents", status: 200,
			response: `{"count":1,"value":[{"id":13,"name":"linux-x64-0001","runnerScaleSetId":7}]}`,
			check: func(r *http.Request) {
				checkValue(t, "runner lookup name", r.URL.Query().Get("agentName"), "linux-x64-0001")
			}},
	)
	client := newPAT(t, scope, wire)
	ctx := scaleset.WithRequestRetry(t.Context(), func(config *scaleset.RetryConfig) { config.Max = 0 })
	jit, err := client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: "linux-x64-0000", WorkFolder: "_work"}, 7)
	if err != nil || jit == nil || jit.Runner == nil || jit.Runner.ID != 12 || jit.EncodedJITConfig != "fixture-config" {
		t.Fatalf("unexpected JIT fixture response: %v", err)
	}
	_, err = client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: "linux-x64-0001", WorkFolder: "_work"}, 7)
	if !errors.Is(err, lostResponse) {
		t.Fatalf("JIT error = %v, want lost response", err)
	}
	runner, err := client.GetRunnerByName(t.Context(), "linux-x64-0001")
	if err != nil || runner == nil || runner.ID != 13 || runner.RunnerScaleSetID != 7 {
		t.Fatalf("runner = %+v, error = %v", runner, err)
	}
}
