// Package api serves the REST surface used to declare deployments and to
// inspect cluster state.
//
// SECURITY: these endpoints are unauthenticated, and a deployment carries an
// exec_path that worker nodes will execute. Anyone who can reach this port can
// therefore run an arbitrary binary on every worker. That matches the project
// spec, which targets an isolated virtual test cluster, but it means the API
// must never be bound to a public interface. Adding authentication is a
// prerequisite for running this anywhere untrusted.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"mini-k8s/internal/events"
	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/scheduler"
	"mini-k8s/internal/schema"
)

// maxRequestBody caps request bodies so a malformed or hostile client cannot
// make the server allocate without bound.
const maxRequestBody = 64 * 1024

// Server owns the HTTP surface of the control plane.
type Server struct {
	client    *redisclient.Client
	store     *DeploymentStore
	scheduler *scheduler.Scheduler
	logger    *logging.Logger
}

// NewServer builds an API server.
func NewServer(client *redisclient.Client, logger *logging.Logger) *Server {
	return &Server{
		client:    client,
		store:     NewDeploymentStore(client),
		scheduler: scheduler.New(client, logger),
		logger:    logger,
	}
}

// Routes returns the HTTP handler for the API.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /deployments", s.handleCreateDeployment)
	mux.HandleFunc("GET /deployments", s.handleListDeployments)
	mux.HandleFunc("GET /deployments/{name}", s.handleGetDeployment)
	mux.HandleFunc("POST /deployments/{name}/scale", s.handleScaleDeployment)
	mux.HandleFunc("GET /nodes", s.handleListNodes)
	mux.HandleFunc("POST /deployments/{name}/schedule", s.handleTriggerSchedule)
	mux.HandleFunc("GET /state", s.handleState)
	mux.HandleFunc("GET /events", s.handleEvents)
	// Registered last and matched last: "GET /" is ServeMux's catch-all.
	mux.HandleFunc("GET /", s.handleDashboard)
	return mux
}

// handleHealthz reports whether the server can still reach Redis.
//
// It probes Redis rather than returning a bare 200: an api-server that is
// running but cannot read state is not actually serving, and reporting it
// healthy would hide the real fault.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.client.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"reason": "redis unreachable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCreateDeployment validates and stores a deployment spec, then
// publishes an event so reconciliation starts without waiting for the sweep.
func (s *Server) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	var req DeploymentRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := req.Validate(); err != nil {
		var verr *ValidationError
		if errors.As(err, &verr) {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error":    "validation failed",
				"problems": verr.Problems,
			})
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	dep, existed, err := s.store.Apply(r.Context(), &req)
	if err != nil {
		s.logger.Error(r.Context(), "deployment_write_failed", err.Error(),
			logging.DeploymentID(req.Name))
		writeError(w, http.StatusInternalServerError, "could not store deployment")
		return
	}

	action := "created"
	status := http.StatusCreated
	if existed {
		action = "updated"
		status = http.StatusOK
	}

	s.logger.Info(r.Context(), "deployment_"+action,
		fmt.Sprintf("%s with desired=%d bounds=[%d,%d] request=%dm/%dMB",
			action, dep.DesiredReplicas, dep.MinReplicas, dep.MaxReplicas,
			dep.CPURequest, dep.MemRequest),
		logging.DeploymentID(dep.Name))

	// The spec is already durable, so a failed publish only delays
	// reconciliation until the next sweep. It must not fail the request.
	if err := events.Publish(r.Context(), s.client, events.DeploymentEvent{
		Event:      events.EventUpdate,
		Deployment: dep.Name,
		Desired:    dep.DesiredReplicas,
	}); err != nil {
		s.logger.Warn(r.Context(), "event_publish_failed",
			fmt.Sprintf("reconciliation will pick this up on the next sweep: %v", err),
			logging.DeploymentID(dep.Name))
	}

	writeJSON(w, status, dep)
}

// handleGetDeployment returns a single deployment.
func (s *Server) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	dep, found, err := s.store.Get(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, fmt.Sprintf("deployment %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, dep)
}

// handleListDeployments returns every deployment.
func (s *Server) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	deployments, err := s.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"deployments": deployments,
		"count":       len(deployments),
	})
}

// scaleRequest is the body accepted when overriding a replica count by hand.
type scaleRequest struct {
	DesiredReplicas *int `json:"desired_replicas"`
}

// handleScaleDeployment sets desired_replicas directly.
//
// This is the manual counterpart to the autoscaler, used to drive
// reconciliation in tests and from the dashboard. The value is clamped to the
// deployment's own bounds, so a manual override cannot escape the limits the
// spec declared.
func (s *Server) handleScaleDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var req scaleRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.DesiredReplicas == nil {
		writeError(w, http.StatusBadRequest, "desired_replicas is required")
		return
	}
	if *req.DesiredReplicas < 0 {
		writeError(w, http.StatusBadRequest, "desired_replicas must not be negative")
		return
	}

	dep, err := s.store.SetDesiredReplicas(r.Context(), name, *req.DesiredReplicas)
	if err != nil {
		_, found, getErr := s.store.Get(r.Context(), name)
		if getErr == nil && !found {
			writeError(w, http.StatusNotFound, fmt.Sprintf("deployment %q not found", name))
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.logger.Info(r.Context(), "deployment_scaled_manually",
		fmt.Sprintf("desired_replicas set to %d (requested %d, clamped to [%d,%d])",
			dep.DesiredReplicas, *req.DesiredReplicas, dep.MinReplicas, dep.MaxReplicas),
		logging.DeploymentID(name))

	if err := events.Publish(r.Context(), s.client, events.DeploymentEvent{
		Event:      events.EventUpdate,
		Deployment: dep.Name,
		Desired:    dep.DesiredReplicas,
	}); err != nil {
		s.logger.Warn(r.Context(), "event_publish_failed",
			fmt.Sprintf("reconciliation will pick this up on the next sweep: %v", err),
			logging.DeploymentID(dep.Name))
	}

	writeJSON(w, http.StatusOK, dep)
}

// handleListNodes reports each registered node with its capacity and whether
// its liveness lease is currently present.
func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	nodeIDs, err := s.client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sort.Strings(nodeIDs)

	type nodeView struct {
		NodeID       string `json:"node_id"`
		Ready        bool   `json:"ready"`
		TotalCPU     int    `json:"total_cpu"`
		TotalMem     int    `json:"total_mem"`
		AllocatedCPU int    `json:"allocated_cpu"`
		AllocatedMem int    `json:"allocated_mem"`
	}

	nodes := make([]nodeView, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		view := nodeView{NodeID: nodeID}

		// A registered node whose lease has expired is dead but not yet
		// evicted, so it is reported as not ready rather than omitted.
		ready, err := s.client.Exists(ctx, schema.NodeStatusKey(nodeID))
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		view.Ready = ready

		if fields, err := s.client.HashGet(ctx, schema.NodeCapacityKey(nodeID)); err == nil && len(fields) > 0 {
			if capacity, err := schema.MapToNodeCapacity(fields); err == nil {
				view.TotalCPU = capacity.TotalCPU
				view.TotalMem = capacity.TotalMem
				view.AllocatedCPU = capacity.AllocatedCPU
				view.AllocatedMem = capacity.AllocatedMem
			}
		}
		nodes = append(nodes, view)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"nodes": nodes, "count": len(nodes)})
}

// handleTriggerSchedule runs one placement pass for a deployment.
//
// This exists so placement is exercisable before the Replica Controller loop
// is built. It schedules only the shortfall between desired and existing pods,
// so repeated calls converge instead of piling on duplicate replicas.
func (s *Server) handleTriggerSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	dep, found, err := s.store.Get(ctx, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, fmt.Sprintf("deployment %q not found", name))
		return
	}

	pods, err := PodsForDeployment(ctx, s.client, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	existing := 0
	for _, pod := range pods {
		if schema.ConsumesCapacity(pod.Status) {
			existing++
		}
	}

	shortfall := dep.DesiredReplicas - existing
	created, err := s.scheduler.ScheduleReplicas(ctx, dep, shortfall)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"deployment":       name,
		"desired_replicas": dep.DesiredReplicas,
		"existing_pods":    existing,
		"created_pods":     created,
		"created":          len(created),
	})
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// decodeJSON reads a size-limited JSON body and rejects unknown fields, so a
// misspelled field is reported instead of being silently ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("could not parse request body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// The status line is already sent, so this can only be logged.
		fmt.Printf("api: response encode failed: %v\n", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// PodsForDeployment returns every pod belonging to a deployment, ordered by
// pod ID. Exposed for reuse by the controllers and the dashboard.
func PodsForDeployment(ctx context.Context, client *redisclient.Client, deployment string) ([]*schema.PodSpec, error) {
	keys, err := client.ScanKeys(ctx, schema.DeploymentPodsPattern(deployment))
	if err != nil {
		return nil, fmt.Errorf("scan pods for %s: %w", deployment, err)
	}

	pods := make([]*schema.PodSpec, 0, len(keys))
	for _, key := range keys {
		fields, err := client.HashGet(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read pod %s: %w", key, err)
		}
		if len(fields) == 0 {
			continue // deleted between scan and read
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			continue // a malformed pod must not break the listing
		}
		pods = append(pods, pod)
	}
	return pods, nil
}
