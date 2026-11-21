package sdkcontract

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/actions/scaleset"

	"github.com/knightfall/scaleset-example-aws/githubsets"
	"github.com/knightfall/scaleset-example-aws/lifecycle"
)

func TestSessionAdapterRefreshesAndPassesOnlySanitizedJobFacts(t *testing.T) {
	const scope = "https://github.com/enterprises/corp"
	sessionDocument := func(token string) string {
		return fmt.Sprintf(`{
			"sessionId":%q, "ownerName":"fixture-owner",
			"messageQueueUrl":"https://queue.example.invalid/messages",
			"messageQueueAccessToken":%q,
			"statistics":{"totalAssignedJobs":2,"totalRunningJobs":1}
		}`, sessionID, token)
	}
	checkPoll := func(token string) func(*http.Request) {
		return func(r *http.Request) {
			checkValue(t, "queue authorization", r.Header.Get("Authorization"), "Bearer "+token)
			checkValue(t, "capacity", r.Header.Get(scaleset.HeaderScaleSetMaxCapacity), "0")
			checkValue(t, "cursor", r.URL.Query().Get("lastMessageId"), "18")
		}
	}
	body, err := json.Marshal([]map[string]any{
		{"messageType": "JobAvailable", "runnerRequestId": 101, "acquireJobUrl": "https://fixture-secret.invalid/acquire"},
		{"messageType": "JobAssigned", "runnerRequestId": 102, "jobDisplayName": "fixture-secret-job"},
		{"messageType": "JobStarted", "runnerRequestId": 102, "runnerId": 99, "runnerName": "fixture-secret-runner"},
		{"messageType": "JobCompleted", "runnerRequestId": 103, "runnerId": 12, "finishTime": "2026-11-21T00:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := newScript(t,
		registrationExchange(t, "api.github.com", "/enterprises/corp/actions/runners/registration-token", "Bearer fixture-pat"),
		adminExchange(t, "api.github.com", "/actions/runner-registration", scope),
		exchange{method: "POST", path: sessionPath, status: 200, response: sessionDocument("queue-old")},
		exchange{method: "GET", path: queuePath, status: 401, check: checkPoll("queue-old")},
		exchange{method: "PATCH", path: sessionPath + "/" + sessionID, status: 200, response: sessionDocument("queue-new")},
		exchange{method: "GET", path: queuePath, status: 200, check: checkPoll("queue-new"),
			response: fmt.Sprintf(`{"messageId":19,"messageType":"RunnerScaleSetJobMessages","body":%q}`, string(body))},
		exchange{method: "POST", path: scaleSetPath + "/7/acquirejobs", err: errors.New("fixture-secret-acquire"),
			check: func(r *http.Request) { checkJSON(t, r, `[101]`) }},
		exchange{method: "DELETE", path: queuePath + "/19", err: errors.New("fixture-secret-ack")},
		exchange{method: "GET", path: queuePath, status: 202, check: checkPoll("queue-new")},
		exchange{method: "DELETE", path: queuePath + "/0", status: 204},
	)
	client, err := newPAT(t, scope, wire).MessageSessionClient(t.Context(), 7, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	session, err := githubsets.AdaptSession(client)
	if err != nil {
		t.Fatal(err)
	}
	if initial := session.InitialDemand(); session.ID() != sessionID || initial == nil || initial.Assigned != 2 || initial.Running != 1 {
		t.Fatalf("initial statistics = %+v", initial)
	}
	delivery, err := session.Poll(t.Context(), 18, 0)
	if err != nil || delivery == nil || delivery.MessageID != 19 || delivery.Stats != nil ||
		!slices.Equal(delivery.Offers, []int64{101}) || len(delivery.Jobs) != 3 ||
		delivery.Jobs[0].Phase != lifecycle.JobAssigned || delivery.Jobs[1].RunnerID != 99 ||
		delivery.Jobs[2].Phase != lifecycle.JobCompleted || delivery.Jobs[2].OccurredAt.IsZero() {
		t.Fatalf("delivery = %+v, error = %v", delivery, err)
	}
	encoded, err := json.Marshal(delivery)
	if err != nil || strings.Contains(string(encoded), "fixture-secret") {
		t.Fatalf("delivery included raw fields: %s, error = %v", encoded, err)
	}
	if _, err := session.Acquire(t.Context(), []int64{101}); !errors.Is(err, githubsets.ErrRequest) || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("acquire error = %v", err)
	}
	if err := session.Acknowledge(t.Context(), 19); !errors.Is(err, githubsets.ErrRequest) || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("ack error = %v", err)
	}
	if message, err := session.Poll(t.Context(), 18, 0); message != nil || err != nil || session.ID() != sessionID {
		t.Fatalf("empty poll = %+v, error = %v", message, err)
	}
	if err := session.Acknowledge(t.Context(), 0); err != nil {
		t.Fatalf("acknowledgement for message zero: %v", err)
	}
}
