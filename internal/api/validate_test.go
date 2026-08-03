package api

import (
	"errors"
	"strings"
	"testing"
)

// intPtr is a helper for building requests where absent and zero must differ.
func intPtr(v int) *int { return &v }

// validRequest returns a request that passes validation, for tests that want
// to invalidate exactly one field.
func validRequest() *DeploymentRequest {
	return &DeploymentRequest{
		Name:             "payment-api",
		MinReplicas:      intPtr(2),
		MaxReplicas:      intPtr(6),
		TargetCPUPercent: intPtr(50),
		CPURequest:       intPtr(150),
		MemRequest:       intPtr(128),
		ExecPath:         "/usr/local/bin/payment-api",
		Port:             intPtr(8080),
	}
}

func TestValidateAcceptsWellFormedRequest(t *testing.T) {
	if err := validRequest().Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

func TestValidateRejectsMissingRequiredFields(t *testing.T) {
	// Every field is required; absent must be reported, not defaulted.
	empty := &DeploymentRequest{}
	err := empty.Validate()
	if err == nil {
		t.Fatal("empty request should be rejected")
	}

	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}

	// All problems must be reported at once so a user fixing a form does not
	// have to resubmit once per mistake.
	required := []string{
		"name", "exec_path", "min_replicas", "max_replicas",
		"target_cpu_percentage", "cpu_request", "mem_request", "port",
	}
	joined := strings.Join(verr.Problems, "; ")
	for _, field := range required {
		if !strings.Contains(joined, field) {
			t.Errorf("missing %q not reported; got: %s", field, joined)
		}
	}
}

// A pointer field set to 0 must be rejected as out of bounds, not silently
// treated as unset and defaulted to a working value.
func TestValidateDistinguishesExplicitZeroFromAbsent(t *testing.T) {
	req := validRequest()
	req.MinReplicas = intPtr(0)

	err := req.Validate()
	if err == nil {
		t.Fatal("min_replicas=0 should be rejected")
	}
	if !strings.Contains(err.Error(), "min_replicas") {
		t.Errorf("error should name min_replicas, got: %v", err)
	}
}

func TestValidateRejectsMinAboveMax(t *testing.T) {
	req := validRequest()
	req.MinReplicas = intPtr(8)
	req.MaxReplicas = intPtr(3)

	err := req.Validate()
	if err == nil {
		t.Fatal("min_replicas above max_replicas should be rejected")
	}
	if !strings.Contains(err.Error(), "min_replicas") || !strings.Contains(err.Error(), "max_replicas") {
		t.Errorf("error should mention both bounds, got: %v", err)
	}
}

func TestValidateAcceptsMinEqualToMax(t *testing.T) {
	req := validRequest()
	req.MinReplicas = intPtr(3)
	req.MaxReplicas = intPtr(3)

	if err := req.Validate(); err != nil {
		t.Fatalf("a fixed replica count (min == max) is valid, got: %v", err)
	}
}

// Names are interpolated into Redis keys and later into nginx upstream names,
// so characters that would break either must be refused.
func TestValidateRejectsUnsafeNames(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"colon collides with the redis key separator", "pay:api"},
		{"whitespace breaks generated nginx config", "pay api"},
		{"braces break generated nginx config", "pay{api}"},
		{"slash would nest the key namespace", "pay/api"},
		{"uppercase is not a dns label", "PaymentAPI"},
		{"leading hyphen", "-payment"},
		{"trailing hyphen", "payment-"},
		{"wildcard", "pay*"},
		{"newline", "pay\napi"},
		{"too long", strings.Repeat("a", maxNameLength+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Name = tc.value
			if err := req.Validate(); err == nil {
				t.Errorf("name %q should be rejected", tc.value)
			}
		})
	}
}

func TestValidateAcceptsLegitimateNames(t *testing.T) {
	for _, name := range []string{"api", "payment-api", "web-tier-2", "a", "a1", strings.Repeat("a", maxNameLength)} {
		req := validRequest()
		req.Name = name
		if err := req.Validate(); err != nil {
			t.Errorf("name %q should be accepted, got: %v", name, err)
		}
	}
}

// A relative exec_path would resolve against whatever working directory each
// agent happens to have, which differs per node.
func TestValidateRequiresAbsoluteExecPath(t *testing.T) {
	for _, path := range []string{"payment-api", "./payment-api", "bin/payment-api", "~/payment-api"} {
		req := validRequest()
		req.ExecPath = path
		err := req.Validate()
		if err == nil {
			t.Errorf("exec_path %q should be rejected as relative", path)
			continue
		}
		if !strings.Contains(err.Error(), "absolute") {
			t.Errorf("exec_path %q: error should explain absoluteness, got: %v", path, err)
		}
	}
}

func TestValidateEnforcesNumericBounds(t *testing.T) {
	cases := []struct {
		field  string
		mutate func(*DeploymentRequest)
	}{
		{"target_cpu_percentage above 100", func(r *DeploymentRequest) { r.TargetCPUPercent = intPtr(101) }},
		{"target_cpu_percentage at 0", func(r *DeploymentRequest) { r.TargetCPUPercent = intPtr(0) }},
		{"negative target_cpu_percentage", func(r *DeploymentRequest) { r.TargetCPUPercent = intPtr(-10) }},
		{"cpu_request at 0", func(r *DeploymentRequest) { r.CPURequest = intPtr(0) }},
		{"cpu_request beyond cap", func(r *DeploymentRequest) { r.CPURequest = intPtr(maxCPURequest + 1) }},
		{"mem_request at 0", func(r *DeploymentRequest) { r.MemRequest = intPtr(0) }},
		{"mem_request beyond cap", func(r *DeploymentRequest) { r.MemRequest = intPtr(maxMemRequest + 1) }},
		{"port 0", func(r *DeploymentRequest) { r.Port = intPtr(0) }},
		{"port above 65535", func(r *DeploymentRequest) { r.Port = intPtr(65536) }},
		{"negative port", func(r *DeploymentRequest) { r.Port = intPtr(-1) }},
		{"replica count beyond cap", func(r *DeploymentRequest) { r.MaxReplicas = intPtr(maxReplicaCount + 1) }},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			req := validRequest()
			tc.mutate(req)
			if err := req.Validate(); err == nil {
				t.Errorf("%s should be rejected", tc.field)
			}
		})
	}
}

func TestValidateAcceptsBoundaryValues(t *testing.T) {
	req := validRequest()
	req.TargetCPUPercent = intPtr(100)
	req.Port = intPtr(65535)
	req.CPURequest = intPtr(maxCPURequest)
	req.MemRequest = intPtr(maxMemRequest)
	req.MinReplicas = intPtr(1)
	req.MaxReplicas = intPtr(maxReplicaCount)

	if err := req.Validate(); err != nil {
		t.Fatalf("inclusive bounds should be accepted, got: %v", err)
	}
}

func TestValidateTrimsSurroundingWhitespace(t *testing.T) {
	req := validRequest()
	req.Name = "  payment-api  "
	req.ExecPath = "  /usr/local/bin/payment-api  "

	if err := req.Validate(); err != nil {
		t.Fatalf("padded values should validate after trimming, got: %v", err)
	}
}
