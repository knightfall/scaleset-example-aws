package lifecycle_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
	"github.com/knightfall/scaleset-example-aws/listener"
	"github.com/knightfall/scaleset-example-aws/runtimeconfig"
)

type supervisorSecrets struct {
	version atomic.Int32
}

func (s *supervisorSecrets) GetSecretValue(_ context.Context, input *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	version := strconv.Itoa(int(s.version.Load()))
	return &secretsmanager.GetSecretValueOutput{
		ARN: input.SecretId, VersionId: aws.String(version),
		SecretString: aws.String(fmt.Sprintf(`{"token":"supervisor-%s"}`, version)),
	}, nil
}

type supervisorRemote struct {
	t                  *testing.T
	events             chan string
	mu                 sync.Mutex
	active             map[string]string
	opens              map[string]int
	polling            map[string]int
	loseClose          bool
	missingBuild       bool
	buildCreates       int
	offered            bool
	invalidateCapacity func()
}

const supervisorSetsPath = "/_apis/runtime/runnerscalesets"

func (r *supervisorRemote) RoundTrip(request *http.Request) (*http.Response, error) {
	r.t.Helper()
	if request.Body != nil {
		defer func() {
			if err := request.Body.Close(); err != nil {
				r.t.Errorf("close request body: %v", err)
			}
		}()
	}
	switch request.URL.Path {
	case "/enterprises/fixture/actions/runners/registration-token":
		if request.Header.Get("Authorization") == "Bearer supervisor-2" {
			r.record("rejected-candidate")
			return fixtureReply(http.StatusForbidden, "{}"), nil
		}
		return fixtureReply(http.StatusCreated, `{"token":"registration-fixture","expires_at":"2099-01-01T00:00:00Z"}`), nil
	case "/actions/runner-registration":
		payload := fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())
		token := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fixture"
		return fixtureReply(http.StatusOK, fmt.Sprintf(`{"url":"https://actions.example.invalid","token":%q}`, token)), nil
	case "/_apis/runtime/runnergroups/":
		return fixtureReply(http.StatusOK, `{"count":1,"value":[{"id":19,"name":"Build"}]}`), nil
	case supervisorSetsPath:
		name := request.URL.Query().Get("name")
		if request.Method == http.MethodPost {
			if !r.missingBuild {
				return nil, errors.New("unexpected scale set creation")
			}
			r.buildCreates++
			return fixtureReply(http.StatusCreated, `{"id":7}`), nil
		}
		if r.missingBuild && name == "linux" && r.buildCreates == 0 {
			return fixtureReply(http.StatusOK, `{"count":0,"value":[]}`), nil
		}
		id := 7
		if name == "other" {
			id = 8
		}
		body := fmt.Sprintf(`{"count":1,"value":[{"id":%d,"name":%q,"runnerGroupId":19,"labels":[{"name":%q}]}]}`, id, name, name)
		return fixtureReply(http.StatusOK, body), nil
	}
	if set, found := strings.CutPrefix(request.URL.Path, "/messages/"); found {
		if request.Method == http.MethodDelete && set == "7/1" && r.invalidateCapacity != nil {
			r.record("acked:7")
			return fixtureReply(http.StatusNoContent, ""), nil
		}
		return r.poll(request, set)
	}
	path := strings.TrimPrefix(request.URL.Path, supervisorSetsPath+"/")
	set, action, _ := strings.Cut(path, "/")
	switch {
	case request.Method == http.MethodPost && action == "sessions":
		return r.open(request, set)
	case request.Method == http.MethodDelete && strings.HasPrefix(action, "sessions/"):
		return r.close(set, strings.TrimPrefix(action, "sessions/"))
	default:
		r.t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		return nil, errors.New("unexpected scripted request")
	}
}

func (r *supervisorRemote) open(request *http.Request, set string) (*http.Response, error) {
	var payload struct{ OwnerName string }
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[set] != "" || (set != "7" && set != "8") {
		r.t.Errorf("overlapping or unknown session for set %s", set)
		return nil, errors.New("fixture detected overlapping sessions")
	}
	r.opens[set]++
	id := fmt.Sprintf("00000000-0000-0000-000%s-%012d", set, r.opens[set])
	r.active[set] = id
	r.record(fmt.Sprintf("opened:%s:%d", set, r.opens[set]))
	body := fmt.Sprintf(`{"sessionId":%q,"ownerName":%q,"runnerScaleSet":{"id":%s},
		"messageQueueUrl":"https://queue.example.invalid/messages/%s","messageQueueAccessToken":"fixture-token",
		"statistics":{"totalAssignedJobs":1}}`, id, payload.OwnerName, set, set)
	return fixtureReply(http.StatusOK, body), nil
}

func (r *supervisorRemote) poll(request *http.Request, set string) (*http.Response, error) {
	r.mu.Lock()
	if r.active[set] == "" {
		r.t.Errorf("poll of closed session for set %s", set)
	}
	offer := r.invalidateCapacity != nil && !r.offered
	r.offered = r.offered || offer
	r.polling[set]++
	r.mu.Unlock()
	r.record("poll:" + set)
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.polling[set]--
	}()
	if offer {
		if hint := request.Header.Get("X-ScaleSetMaxCapacity"); hint != "3" {
			r.t.Errorf("verified backend advertised capacity %q", hint)
		}
		r.invalidateCapacity()
		return fixtureReply(http.StatusOK, `{"messageId":1,"messageType":"RunnerScaleSetJobMessages",
			"body":"[{\"messageType\":\"JobAvailable\",\"runnerRequestId\":101}]","statistics":{"totalAssignedJobs":1}}`), nil
	}
	if hint := request.Header.Get("X-ScaleSetMaxCapacity"); hint != "0" {
		r.t.Errorf("unverified backend advertised capacity %q", hint)
	}
	timer := time.NewTimer(30 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-request.Context().Done():
		return nil, request.Context().Err()
	case <-timer.C:
		return fixtureReply(http.StatusAccepted, ""), nil
	}
}

func (r *supervisorRemote) close(set, id string) (*http.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[set] != id || r.polling[set] != 0 {
		r.t.Errorf("close of wrong session or before poll joined for set %s", set)
		return nil, errors.New("fixture detected unsafe shutdown")
	}
	delete(r.active, set)
	r.record("closed:" + set)
	if r.loseClose {
		return nil, errors.New("fixture lost session close response")
	}
	return fixtureReply(http.StatusNoContent, ""), nil
}

func (r *supervisorRemote) record(event string) {
	select {
	case r.events <- event:
	default:
	}
}

func useProfilePrefixQueries(db *database) {
	db.query = func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if !aws.ToBool(input.ConsistentRead) || aws.ToString(input.IndexName) != "" {
			return nil, errors.New("shutdown must inspect effects in the base table")
		}
		partition := stringValue(input.ExpressionAttributeValues[":pk"])
		prefix := stringValue(input.ExpressionAttributeValues[":prefix"])
		db.mu.Lock()
		defer db.mu.Unlock()
		page := &dynamodb.QueryOutput{}
		for key, item := range db.items {
			if key.PK == partition && strings.HasPrefix(key.SK, prefix) {
				page.Items = append(page.Items, item)
			}
		}
		return page, nil
	}
}

func supervisedFixture(t *testing.T, loseClose bool) (*lifecycle.Store, lifecycle.Lease, *supervisorSecrets, *supervisorRemote, listener.Supervisor, *database) {
	t.Helper()
	ledger, db := store(t)
	useProfilePrefixQueries(db)
	timing := listener.Timing{
		LeaseTTL: 2 * time.Second, OperationTimeout: time.Second, PollTimeout: time.Second,
		CredentialInterval: 75 * time.Millisecond, MaxDemandAge: time.Minute, ShutdownTimeout: time.Second,
	}
	lease, err := ledger.AcquireLease(t.Context(), "dev", "controller", time.Now(), timing.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	secrets := &supervisorSecrets{}
	secrets.version.Store(1)
	remote := &supervisorRemote{
		t: t, events: make(chan string, 128), active: make(map[string]string),
		opens: make(map[string]int), polling: make(map[string]int), loseClose: loseClose,
	}
	supervisor := listener.Supervisor{
		Store: ledger, Timing: timing, ConfigurationGeneration: 1, Logger: slog.New(slog.DiscardHandler),
		Loader: githubsets.Loader{Secrets: secrets, WrapTransport: func(http.RoundTripper) http.RoundTripper { return remote }},
	}
	return ledger, lease, secrets, remote, supervisor, db
}

func waitForSupervisorEvents(t *testing.T, events <-chan string, requested ...string) {
	t.Helper()
	pending := make(map[string]bool)
	for _, event := range requested {
		pending[event] = true
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for len(pending) > 0 {
		select {
		case event := <-events:
			delete(pending, event)
		case <-timer.C:
			t.Fatalf("missing supervisor events: %v", pending)
		}
	}
}

func TestSupervisorRotatesIndependentProfilesAndReleasesAfterShutdown(t *testing.T) {
	ledger, lease, secrets, remote, supervisor, _ := supervisedFixture(t, false)
	artifact := githubCreationConfig()
	profile := artifact.Config.Spec.Profiles[0]
	profile.ID = "other"
	profile.GitHub.ScaleSet = "other"
	profile.GitHub.Labels = []string{"other"}
	artifact.Config.Spec.Profiles = append(artifact.Config.Spec.Profiles, profile)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = supervisor.Run(ctx, artifact, lease)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("supervisor still running after cleanup")
		}
	})
	waitForSupervisorEvents(t, remote.events, "poll:7", "poll:8")
	secrets.version.Store(2)
	waitForSupervisorEvents(t, remote.events, "rejected-candidate")
	remote.mu.Lock()
	if len(remote.active) != 2 || remote.opens["7"] != 1 || remote.opens["8"] != 1 {
		t.Error("rejected credential interrupted a live profile")
	}
	remote.mu.Unlock()
	secrets.version.Store(3)
	waitForSupervisorEvents(t, remote.events, "opened:7:2", "opened:8:2")
	// The lease should renew while both lanes continue polling.
	expiry := time.NewTimer(4 * time.Second)
	defer expiry.Stop()
	updates := time.NewTicker(25 * time.Millisecond)
	defer updates.Stop()
	for {
		current, err := ledger.Lease(t.Context(), "dev")
		if err != nil {
			t.Fatal(err)
		}
		if current != nil && current.ValidUntil.After(lease.ValidUntil) {
			break
		}
		select {
		case <-updates.C:
		case <-expiry.C:
			t.Fatal("lease was not renewed")
		}
	}
	cancel()
	select {
	case <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
	current, err := ledger.Lease(t.Context(), "dev")
	if err != nil || current == nil || current.Phase != lifecycle.LeaseReleased {
		t.Fatalf("lease after shutdown = %+v, error = %v", current, err)
	}
	if len(remote.active) != 0 {
		t.Fatalf("shutdown left sessions active: %v", remote.active)
	}
}

func TestSupervisorKeepsLeaseWhenSessionCloseIsUnconfirmed(t *testing.T) {
	ledger, lease, _, remote, supervisor, _ := supervisedFixture(t, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, githubCreationConfig(), lease) }()
	waitForSupervisorEvents(t, remote.events, "poll:7")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, githubsets.ErrSessionUnconfirmed) {
			t.Fatalf("shutdown error = %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
	current, err := ledger.Lease(t.Context(), "dev")
	if err != nil || current == nil || current.Phase != lifecycle.LeaseActive {
		t.Fatalf("ambiguous close released lease: %+v, error = %v", current, err)
	}
	owner, err := ledger.SessionOwnership(t.Context(), "dev", "build")
	if err != nil || owner.Phase != lifecycle.SessionClosing {
		t.Fatalf("lost close was not retained in ownership: %+v, error = %v", owner, err)
	}
}

func TestSupervisorRechecksCapacityAfterPollBeforeAdmitting(t *testing.T) {
	ledger, lease, _, remote, supervisor, _ := supervisedFixture(t, false)
	artifact := githubCreationConfig()
	artifact.Config.Spec.Profiles[0].Capacity.MaxRunners = 3
	var proven atomic.Bool
	proven.Store(true)
	remote.invalidateCapacity = func() { proven.Store(false) }
	supervisor.Capacity = func(context.Context, runtimeconfig.Profile) (*listener.Capacity, error) {
		if !proven.Load() {
			return nil, nil
		}
		return &listener.Capacity{Maximum: 3}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, artifact, lease) }()
	waitForSupervisorEvents(t, remote.events, "acked:7")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
	reservation, err := ledger.Get(t.Context(), lifecycle.ProfileKey("dev", "build", "ACQUIRE#101"))
	if err != nil || reservation != nil {
		t.Fatalf("lost capacity did not close admission: %+v, %v", reservation, err)
	}
}

func TestSupervisorChecksUnfinishedEffectsBeforeReleasingLease(t *testing.T) {
	ledger, lease, _, remote, supervisor, _ := supervisedFixture(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	now := time.Now()
	if _, err := ledger.PlanEffect(t.Context(), lease, "build", lifecycle.Effect{
		ID: "pending-effect", Kind: "test", Target: "fixture",
		RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(time.Second),
	}, now); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, githubCreationConfig(), lease) }()
	waitForSupervisorEvents(t, remote.events, "poll:7")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, lifecycle.ErrRecoveryRequired) {
			t.Fatalf("shutdown with unsettled effect returned %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
	current, err := ledger.Lease(t.Context(), "dev")
	if err != nil || current == nil || current.Phase != lifecycle.LeaseActive {
		t.Fatalf("unsettled effect released lease: %+v, error = %v", current, err)
	}
}

func TestSupervisorRecoversUnstartedScaleSetCreation(t *testing.T) {
	ledger, lease, _, remote, supervisor, db := supervisedFixture(t, false)
	remote.missingBuild = true
	supervisor.Capacity = func(context.Context, runtimeconfig.Profile) (*listener.Capacity, error) {
		return &listener.Capacity{}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failed := false
	db.beforeWrite = func(input *dynamodb.TransactWriteItemsInput) error {
		for _, tx := range input.TransactItems {
			if tx.Put == nil || keyOf(tx.Put.Item).SK != "OP#create-scale-set" {
				continue
			}
			var effect lifecycle.Effect
			if err := json.Unmarshal([]byte(stringValue(tx.Put.Item["payload"])), &effect); err != nil {
				return err
			}
			if !failed && effect.State == lifecycle.EffectExecuting {
				failed = true
				return errors.New("fixture rejected transition before commit")
			}
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, githubCreationConfig(), lease) }()
	waitForSupervisorEvents(t, remote.events, "poll:7")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("supervisor did not finish shutdown")
	}
	if !failed || remote.buildCreates != 1 {
		t.Fatalf("injected failure = %t, create POSTs = %d, want one POST after the failure", failed, remote.buildCreates)
	}
	effect, err := ledger.Effect(t.Context(), "dev", "build", "create-scale-set")
	if err != nil || effect == nil || effect.State != lifecycle.EffectConfirmed || effect.Evidence != "scale-set:7" {
		t.Fatalf("confirmed effect after retry = %+v, error = %v", effect, err)
	}
}

func TestSupervisorCancelsUnstartedScaleSetCreationBeforeReleasingLease(t *testing.T) {
	for _, state := range []string{lifecycle.EffectPlanned, lifecycle.EffectExecuting} {
		t.Run(state, func(t *testing.T) {
			ledger, lease, _, _, supervisor, _ := supervisedFixture(t, false)
			now := time.Now().Add(-time.Second)
			intent := lifecycle.Effect{
				ID: "create-scale-set", Kind: "create-scale-set", Target: "fixture",
				RequestSHA256: strings.Repeat("ab", 32), Deadline: now.Add(500 * time.Millisecond),
			}
			if _, err := ledger.PlanEffect(t.Context(), lease, "build", intent, now); err != nil {
				t.Fatal(err)
			}
			if state == lifecycle.EffectExecuting {
				if _, err := ledger.BeginEffect(t.Context(), lease, "build", intent.ID, now); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := supervisor.Run(ctx, githubCreationConfig(), lease)
			wantEffect, wantLease := lifecycle.EffectCancelled, lifecycle.LeaseReleased
			if state == lifecycle.EffectExecuting {
				wantEffect, wantLease = lifecycle.EffectExecuting, lifecycle.LeaseActive
				if !errors.Is(err, lifecycle.ErrRecoveryRequired) {
					t.Fatalf("shutdown with executing intent returned %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			effect, err := ledger.Effect(t.Context(), "dev", "build", intent.ID)
			if err != nil || effect == nil || effect.State != wantEffect {
				t.Fatalf("effect after shutdown = %+v, want %s, error = %v", effect, wantEffect, err)
			}
			current, err := ledger.Lease(t.Context(), "dev")
			if err != nil || current == nil || current.Phase != wantLease {
				t.Fatalf("lease after shutdown = %+v, want %s, error = %v", current, wantLease, err)
			}
		})
	}
}
