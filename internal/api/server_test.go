package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// newTestServer returns a running test HTTP server and a unique deployment name.
func newTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	client, err := redisclient.New(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr(), err)
	}

	srv := httptest.NewServer(NewServer(client, logging.New("test", nil)).Routes())
	name := fmt.Sprintf("h-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		client.DeleteKey(context.Background(), schema.DeploymentKey(name))
		srv.Close()
		client.Close()
	})
	return srv, name
}

// postJSON sends a JSON body and returns the status and decoded response.
func postJSON(t *testing.T, url, body string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	var decoded map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

func getJSON(t *testing.T, url string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var decoded map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

// validBody returns a well-formed deployment JSON body.
func validBody(name string) string {
	return fmt.Sprintf(`{
		"name": %q,
		"min_replicas": 2,
		"max_replicas": 6,
		"target_cpu_percentage": 50,
		"cpu_request": 150,
		"mem_request": 128,
		"exec_path": "/usr/local/bin/payment-api",
		"port": 8080
	}`, name)
}

// healthz probes Redis rather than returning a bare 200, so a server that
// cannot read state does not report itself healthy.
func TestHealthzReportsOKWhenRedisReachable(t *testing.T) {
	srv, _ := newTestServer(t)
	status, body := getJSON(t, srv.URL+"/healthz")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body["status"] != "ok" {
		t.Errorf("status field = %v, want ok", body["status"])
	}
}

func TestCreateDeploymentReturns201ThenUpdateReturns200(t *testing.T) {
	srv, name := newTestServer(t)

	status, body := postJSON(t, srv.URL+"/deployments", validBody(name))
	if status != http.StatusCreated {
		t.Fatalf("create status = %d, want 201 (body: %v)", status, body)
	}
	if body["name"] != name {
		t.Errorf("name = %v, want %q", body["name"], name)
	}
	if body["desired_replicas"] != float64(2) {
		t.Errorf("desired_replicas = %v, want 2", body["desired_replicas"])
	}

	// A repeat POST is a defined, idempotent update rather than a second
	// deployment or an error.
	status, body = postJSON(t, srv.URL+"/deployments", validBody(name))
	if status != http.StatusOK {
		t.Fatalf("update status = %d, want 200 (body: %v)", status, body)
	}
}

func TestCreateDeploymentRejectsInvalidSpecWithAllProblems(t *testing.T) {
	srv, _ := newTestServer(t)

	status, body := postJSON(t, srv.URL+"/deployments", `{"name": "Bad Name", "min_replicas": 9, "max_replicas": 2}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}

	problems, ok := body["problems"].([]interface{})
	if !ok {
		t.Fatalf("response should carry a problems list, got: %v", body)
	}
	// The name is invalid, min exceeds max, and several fields are absent, so
	// a single response should surface more than one problem.
	if len(problems) < 3 {
		t.Errorf("expected several problems reported at once, got %d: %v", len(problems), problems)
	}
}

// A misspelled field must be reported rather than silently ignored, which
// would leave the user thinking a value took effect when it did not.
func TestCreateDeploymentRejectsUnknownFields(t *testing.T) {
	srv, name := newTestServer(t)
	body := fmt.Sprintf(`{
		"name": %q, "min_replicas": 2, "max_replicas": 6,
		"target_cpu_percentage": 50, "cpu_request": 150, "mem_request": 128,
		"exec_path": "/bin/true", "port": 8080,
		"min_replica": 3
	}`, name)

	status, _ := postJSON(t, srv.URL+"/deployments", body)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for an unknown field", status)
	}
}

func TestCreateDeploymentRejectsMalformedJSON(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, body := range []string{`{"name": `, `not json at all`, ``} {
		status, _ := postJSON(t, srv.URL+"/deployments", body)
		if status != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, status)
		}
	}
}

// An oversized body must be refused rather than allocated.
func TestCreateDeploymentRejectsOversizedBody(t *testing.T) {
	srv, _ := newTestServer(t)
	huge := fmt.Sprintf(`{"name": "a", "exec_path": %q}`, "/"+strings.Repeat("x", maxRequestBody+1024))

	status, _ := postJSON(t, srv.URL+"/deployments", huge)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for an oversized body", status)
	}
}

func TestGetDeploymentReturnsSpecOr404(t *testing.T) {
	srv, name := newTestServer(t)

	if status, _ := postJSON(t, srv.URL+"/deployments", validBody(name)); status != http.StatusCreated {
		t.Fatalf("setup create failed with %d", status)
	}

	status, body := getJSON(t, srv.URL+"/deployments/"+name)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body["exec_path"] != "/usr/local/bin/payment-api" {
		t.Errorf("exec_path = %v, did not round-trip", body["exec_path"])
	}

	status, _ = getJSON(t, srv.URL+"/deployments/"+name+"-absent")
	if status != http.StatusNotFound {
		t.Errorf("missing deployment status = %d, want 404", status)
	}
}

func TestListDeploymentsIncludesCreatedDeployment(t *testing.T) {
	srv, name := newTestServer(t)
	if status, _ := postJSON(t, srv.URL+"/deployments", validBody(name)); status != http.StatusCreated {
		t.Fatalf("setup create failed with %d", status)
	}

	status, body := getJSON(t, srv.URL+"/deployments")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	list, ok := body["deployments"].([]interface{})
	if !ok {
		t.Fatalf("response should carry a deployments array, got: %v", body)
	}
	found := false
	for _, item := range list {
		if d, ok := item.(map[string]interface{}); ok && d["name"] == name {
			found = true
		}
	}
	if !found {
		t.Errorf("deployment %q missing from listing of %d", name, len(list))
	}
}

// A manual override must not escape the bounds the spec declared.
func TestScaleEndpointClampsToDeclaredBounds(t *testing.T) {
	srv, name := newTestServer(t)
	if status, _ := postJSON(t, srv.URL+"/deployments", validBody(name)); status != http.StatusCreated {
		t.Fatalf("setup create failed with %d", status)
	}
	url := srv.URL + "/deployments/" + name + "/scale"

	for _, tc := range []struct {
		requested int
		want      float64
	}{
		{4, 4},  // within [2,6]
		{99, 6}, // clamped down to max
		{0, 2},  // lifted up to min
	} {
		status, body := postJSON(t, url, fmt.Sprintf(`{"desired_replicas": %d}`, tc.requested))
		if status != http.StatusOK {
			t.Fatalf("scale to %d: status = %d, want 200", tc.requested, status)
		}
		if body["desired_replicas"] != tc.want {
			t.Errorf("scale to %d -> desired %v, want %v", tc.requested, body["desired_replicas"], tc.want)
		}
	}
}

func TestScaleEndpointRejectsBadInputAndMissingDeployment(t *testing.T) {
	srv, name := newTestServer(t)
	if status, _ := postJSON(t, srv.URL+"/deployments", validBody(name)); status != http.StatusCreated {
		t.Fatalf("setup create failed with %d", status)
	}
	url := srv.URL + "/deployments/" + name + "/scale"

	// Absent field: cannot be confused with an explicit 0.
	if status, _ := postJSON(t, url, `{}`); status != http.StatusBadRequest {
		t.Errorf("missing desired_replicas: status = %d, want 400", status)
	}
	if status, _ := postJSON(t, url, `{"desired_replicas": -5}`); status != http.StatusBadRequest {
		t.Errorf("negative desired_replicas: status = %d, want 400", status)
	}

	absent := srv.URL + "/deployments/" + name + "-absent/scale"
	if status, _ := postJSON(t, absent, `{"desired_replicas": 3}`); status != http.StatusNotFound {
		t.Errorf("scaling a missing deployment: status = %d, want 404", status)
	}
}

// Method routing must be explicit: a GET on a POST-only route is a 405.
func TestUnsupportedMethodIsRejected(t *testing.T) {
	srv, name := newTestServer(t)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/deployments/"+name, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
