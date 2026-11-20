package sdkcontract

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type exchange struct {
	method   string
	path     string
	status   int
	response string
	check    func(*http.Request)
	err      error
}

type scriptedHTTP struct {
	t     *testing.T
	steps []exchange
	next  int
}

func newScript(t *testing.T, steps ...exchange) *scriptedHTTP {
	t.Helper()
	script := &scriptedHTTP{t: t, steps: steps}
	t.Cleanup(func() {
		if script.next != len(steps) {
			t.Errorf("observed %d requests, want %d", script.next, len(steps))
		}
	})
	return script
}

func (s *scriptedHTTP) Do(req *http.Request) (*http.Response, error) {
	s.t.Helper()
	if req.Body != nil {
		defer func() {
			if err := req.Body.Close(); err != nil {
				s.t.Errorf("close request body: %v", err)
			}
		}()
	}
	if s.next >= len(s.steps) {
		s.t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
		return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
	}

	step := s.steps[s.next]
	s.next++
	if req.Method != step.method || req.URL.Path != step.path {
		s.t.Errorf("request %d: got %s %s, want %s %s", s.next, req.Method, req.URL.Path, step.method, step.path)
	}
	if step.check != nil {
		step.check(req)
	}
	if step.err != nil {
		return nil, step.err
	}
	return &http.Response{
		StatusCode: step.status,
		Status:     fmt.Sprintf("%d %s", step.status, http.StatusText(step.status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(step.response)),
		Request:    req,
	}, nil
}

func checkJSON(t *testing.T, r *http.Request, expected string) {
	t.Helper()
	if r.Body == nil {
		t.Fatal("expected a JSON request body")
	}
	var got, want any
	if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if err := json.Unmarshal([]byte(expected), &want); err != nil {
		t.Fatalf("invalid fixture: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("request body = %v, want %v", got, want)
	}
}

func checkValue(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", field, got, want)
	}
}
