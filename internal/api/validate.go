package api

import (
	"fmt"
	"regexp"
	"strings"
)

// Validation bounds for a deployment request.
const (
	maxNameLength   = 63   // DNS label limit, keeps names usable as nginx upstream names
	maxReplicaCount = 100  // guards against a typo requesting thousands of processes
	maxCPURequest   = 8000 // 8 cores in millicores
	maxMemRequest   = 65536
	minPort         = 1
	maxPort         = 65535
)

// deploymentNamePattern matches lowercase DNS labels.
//
// Names are restricted because they are interpolated into Redis key patterns
// and, later, nginx upstream block names. A name containing ':' would collide
// with the key separator and make `pod:{deployment}:{id}` ambiguous; one
// containing whitespace or braces could break generated nginx config.
var deploymentNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// DeploymentRequest is the JSON body accepted when declaring a deployment.
//
// Pointer fields distinguish "absent" from "explicitly zero", so a request
// that sets min_replicas to 0 is rejected as out of bounds rather than
// silently treated as unset and defaulted.
type DeploymentRequest struct {
	Name             string `json:"name"`
	MinReplicas      *int   `json:"min_replicas"`
	MaxReplicas      *int   `json:"max_replicas"`
	TargetCPUPercent *int   `json:"target_cpu_percentage"`
	CPURequest       *int   `json:"cpu_request"`
	MemRequest       *int   `json:"mem_request"`
	ExecPath         string `json:"exec_path"`
	Port             *int   `json:"port"`
}

// ValidationError lists everything wrong with a request.
//
// All problems are reported together rather than failing on the first, so a
// user fixing a form does not have to resubmit once per mistake.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid deployment: %s", strings.Join(e.Problems, "; "))
}

// Validate checks a deployment request against the schema's bounds.
func (r *DeploymentRequest) Validate() error {
	var problems []string

	name := strings.TrimSpace(r.Name)
	switch {
	case name == "":
		problems = append(problems, "name is required")
	case len(name) > maxNameLength:
		problems = append(problems, fmt.Sprintf("name must be at most %d characters", maxNameLength))
	case !deploymentNamePattern.MatchString(name):
		problems = append(problems,
			"name must be lowercase alphanumeric with internal hyphens (e.g. payment-api)")
	}

	execPath := strings.TrimSpace(r.ExecPath)
	if execPath == "" {
		problems = append(problems, "exec_path is required")
	} else if !strings.HasPrefix(execPath, "/") {
		// A relative path would resolve against whatever working directory the
		// agent happens to have, which differs per node.
		problems = append(problems, "exec_path must be an absolute path")
	}

	minReplicas, ok := requireInt(&problems, "min_replicas", r.MinReplicas, 1, maxReplicaCount)
	maxReplicas, ok2 := requireInt(&problems, "max_replicas", r.MaxReplicas, 1, maxReplicaCount)
	if ok && ok2 && minReplicas > maxReplicas {
		problems = append(problems,
			fmt.Sprintf("min_replicas (%d) must not exceed max_replicas (%d)", minReplicas, maxReplicas))
	}

	requireInt(&problems, "target_cpu_percentage", r.TargetCPUPercent, 1, 100)
	requireInt(&problems, "cpu_request", r.CPURequest, 1, maxCPURequest)
	requireInt(&problems, "mem_request", r.MemRequest, 1, maxMemRequest)
	requireInt(&problems, "port", r.Port, minPort, maxPort)

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// requireInt validates a required integer field within an inclusive range,
// reporting whether it was usable.
func requireInt(problems *[]string, field string, value *int, min, max int) (int, bool) {
	if value == nil {
		*problems = append(*problems, fmt.Sprintf("%s is required", field))
		return 0, false
	}
	if *value < min || *value > max {
		*problems = append(*problems,
			fmt.Sprintf("%s must be between %d and %d (got %d)", field, min, max, *value))
		return *value, false
	}
	return *value, true
}
