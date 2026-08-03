package api

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// DeploymentStore reads and writes deployment specs in Redis.
type DeploymentStore struct {
	client *redisclient.Client
}

// NewDeploymentStore builds a store over the given Redis client.
func NewDeploymentStore(client *redisclient.Client) *DeploymentStore {
	return &DeploymentStore{client: client}
}

// Get reads a deployment. It reports whether the deployment exists.
func (s *DeploymentStore) Get(ctx context.Context, name string) (*schema.DeploymentSpec, bool, error) {
	fields, err := s.client.HashGet(ctx, schema.DeploymentKey(name))
	if err != nil {
		return nil, false, fmt.Errorf("read deployment %s: %w", name, err)
	}
	if len(fields) == 0 {
		return nil, false, nil
	}
	dep, err := schema.MapToDeployment(fields)
	if err != nil {
		return nil, true, fmt.Errorf("deployment %s is malformed: %w", name, err)
	}
	return dep, true, nil
}

// List returns every deployment, ordered by name.
func (s *DeploymentStore) List(ctx context.Context) ([]*schema.DeploymentSpec, error) {
	keys, err := s.client.ScanKeys(ctx, schema.DeploymentKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan deployments: %w", err)
	}

	deployments := make([]*schema.DeploymentSpec, 0, len(keys))
	for _, key := range keys {
		name := strings.TrimPrefix(key, "deployment:")
		dep, found, err := s.Get(ctx, name)
		if err != nil {
			// One unreadable deployment must not blank the whole listing.
			continue
		}
		if found {
			deployments = append(deployments, dep)
		}
	}
	sort.Slice(deployments, func(i, j int) bool { return deployments[i].Name < deployments[j].Name })
	return deployments, nil
}

// Apply creates or updates a deployment from a validated request. It reports
// whether the deployment already existed.
//
// On create, desired_replicas starts at min_replicas.
//
// On update, the existing desired_replicas is preserved and clamped into the
// new bounds rather than reset to min_replicas. Resetting would undo the
// autoscaler's work: editing an unrelated field (a port, a memory request)
// would collapse a deployment that had legitimately scaled up under load,
// dropping capacity while traffic was still arriving. Clamping still honours
// bounds that were tightened by the edit.
func (s *DeploymentStore) Apply(ctx context.Context, req *DeploymentRequest) (*schema.DeploymentSpec, bool, error) {
	name := strings.TrimSpace(req.Name)

	existing, existed, err := s.Get(ctx, name)
	if err != nil && existed {
		// The stored hash is unreadable. Overwrite it wholesale rather than
		// refusing forever, treating this as a fresh create.
		existing, existed = nil, false
	} else if err != nil {
		return nil, false, err
	}

	dep := &schema.DeploymentSpec{
		Name:             name,
		MinReplicas:      *req.MinReplicas,
		MaxReplicas:      *req.MaxReplicas,
		TargetCPUPercent: *req.TargetCPUPercent,
		CPURequest:       *req.CPURequest,
		MemRequest:       *req.MemRequest,
		ExecPath:         strings.TrimSpace(req.ExecPath),
		Port:             *req.Port,
	}

	if existed && existing != nil {
		dep.CreatedAt = existing.CreatedAt
		dep.LastScaleUpAt = existing.LastScaleUpAt
		dep.LastScaleDownAt = existing.LastScaleDownAt
		dep.DesiredReplicas = clamp(existing.DesiredReplicas, dep.MinReplicas, dep.MaxReplicas)
	} else {
		dep.CreatedAt = strconv.FormatInt(time.Now().Unix(), 10)
		dep.DesiredReplicas = dep.MinReplicas
	}

	if err := s.client.HashSet(ctx, schema.DeploymentKey(name), schema.DeploymentToMap(dep)); err != nil {
		return nil, existed, fmt.Errorf("write deployment %s: %w", name, err)
	}
	return dep, existed, nil
}

// SetDesiredReplicas updates only the desired replica count, clamped to the
// deployment's own bounds.
func (s *DeploymentStore) SetDesiredReplicas(ctx context.Context, name string, desired int) (*schema.DeploymentSpec, error) {
	dep, found, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("deployment %s not found", name)
	}

	dep.DesiredReplicas = clamp(desired, dep.MinReplicas, dep.MaxReplicas)
	if err := s.client.HashSet(ctx, schema.DeploymentKey(name),
		map[string]interface{}{schema.FieldDesiredReplicas: dep.DesiredReplicas}); err != nil {
		return nil, fmt.Errorf("update desired replicas for %s: %w", name, err)
	}
	return dep, nil
}

// clamp constrains v to the inclusive range [min, max].
func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
