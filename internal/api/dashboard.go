package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"mini-k8s/internal/schema"
)

//go:embed dashboard.html
var dashboardFS embed.FS

const (
	// snapshotInterval is how often a connected dashboard receives a fresh
	// snapshot. Each frame is a complete state, so a dropped one costs one
	// interval of staleness rather than leaving the page permanently wrong.
	snapshotInterval = 1 * time.Second

	// logBackfill is how many past log entries a newly connected dashboard
	// receives, so a page opened mid-incident still shows what led up to it.
	logBackfill = 60

	// logBatch bounds how many new entries one frame carries, so a burst of
	// logging cannot make a single frame unbounded.
	logBatch = 100
)

// StateSnapshot is everything the dashboard renders.
type StateSnapshot struct {
	Nodes       []NodeView       `json:"nodes"`
	Deployments []DeploymentView `json:"deployments"`
	Pods        []PodView        `json:"pods"`
	Logs        []LogEntry       `json:"logs,omitempty"`
}

// NodeView is one node's capacity and liveness.
type NodeView struct {
	NodeID       string `json:"node_id"`
	Ready        bool   `json:"ready"`
	TotalCPU     int    `json:"total_cpu"`
	AllocatedCPU int    `json:"allocated_cpu"`
	TotalMem     int    `json:"total_mem"`
	AllocatedMem int    `json:"allocated_mem"`
	PodCount     int    `json:"pod_count"`
}

// DeploymentView is one deployment's declared and observed state.
type DeploymentView struct {
	Name             string `json:"name"`
	DesiredReplicas  int    `json:"desired_replicas"`
	ReadyReplicas    int    `json:"ready_replicas"`
	MinReplicas      int    `json:"min_replicas"`
	MaxReplicas      int    `json:"max_replicas"`
	TargetCPUPercent int    `json:"target_cpu_percentage"`
	ObservedCPU      int    `json:"observed_cpu_percent"`
}

// PodView is one pod's placement and status.
type PodView struct {
	PodID        string `json:"pod_id"`
	Deployment   string `json:"deployment"`
	NodeID       string `json:"node_id"`
	Status       string `json:"status"`
	HostPort     int    `json:"host_port,omitempty"`
	RestartCount int    `json:"restart_count"`
}

// LogEntry is one line from the cluster log stream.
type LogEntry struct {
	ID         string `json:"id"`
	Timestamp  string `json:"timestamp,omitempty"`
	Level      string `json:"level,omitempty"`
	Component  string `json:"component,omitempty"`
	EventType  string `json:"event_type,omitempty"`
	Message    string `json:"message,omitempty"`
	Deployment string `json:"deployment_id,omitempty"`
	PodID      string `json:"pod_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
}

// handleDashboard serves the single-page UI.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// ServeMux's "GET /" matches every unmatched path, so anything that is not
	// the root is a genuine 404 rather than the dashboard.
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	page, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "dashboard asset missing")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is self-contained, so nothing needs to be fetched from anywhere
	// else; saying so means an injected name cannot pull in a remote script.
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page)
}

// handleState returns one snapshot, for polling clients and for debugging.
func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.Snapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

// handleEvents streams snapshots over Server-Sent Events.
//
// SSE rather than WebSockets: the dashboard only ever receives, and SSE
// reconnects on its own, so a control-plane restart does not need a page reload.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Buffering proxies would hold frames back and defeat the point of a stream.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	ctx := r.Context()

	// The first frame carries backfilled history, so a dashboard opened during
	// an incident shows what led up to it rather than starting blank.
	lastLogID, err := s.sendSnapshot(ctx, w, flusher, "", logBackfill)
	if err != nil {
		return
	}

	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Later frames carry only entries newer than the last one sent, or
			// the log panel would receive the same lines repeatedly.
			next, err := s.sendSnapshot(ctx, w, flusher, lastLogID, logBatch)
			if err != nil {
				return
			}
			lastLogID = next
		}
	}
}

// sendSnapshot writes one SSE frame and reports the newest log id it carried.
func (s *Server) sendSnapshot(ctx context.Context, w http.ResponseWriter, flusher http.Flusher,
	sinceLogID string, logCount int64) (string, error) {

	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		// A failed read is not fatal to the stream: the next tick may succeed,
		// and dropping the connection would make a transient Redis blip look
		// like a dead control plane.
		return sinceLogID, nil
	}

	logs, newest, err := s.recentLogs(ctx, sinceLogID, logCount)
	if err == nil {
		snapshot.Logs = logs
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return sinceLogID, nil
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return sinceLogID, err // client went away
	}
	flusher.Flush()

	if newest == "" {
		return sinceLogID, nil
	}
	return newest, nil
}

// Snapshot assembles the full cluster view.
func (s *Server) Snapshot(ctx context.Context) (*StateSnapshot, error) {
	pods, err := s.podViews(ctx)
	if err != nil {
		return nil, err
	}

	podsByNode := make(map[string]int)
	readyByDeployment := make(map[string]int)
	for _, pod := range pods {
		podsByNode[pod.NodeID]++
		if pod.Status == schema.StatusRunning {
			readyByDeployment[pod.Deployment]++
		}
	}

	nodes, err := s.nodeViews(ctx, podsByNode)
	if err != nil {
		return nil, err
	}
	deployments, err := s.deploymentViews(ctx, readyByDeployment)
	if err != nil {
		return nil, err
	}

	return &StateSnapshot{Nodes: nodes, Deployments: deployments, Pods: pods}, nil
}

// podViews reads every pod, sorted so the table does not reshuffle between
// frames: key-scan order is arbitrary, and a jumping table is unreadable.
func (s *Server) podViews(ctx context.Context) ([]PodView, error) {
	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan pods: %w", err)
	}

	views := make([]PodView, 0, len(keys))
	for _, key := range keys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		pod, err := schema.MapToPod(fields)
		if err != nil {
			continue
		}
		views = append(views, PodView{
			PodID: pod.PodID, Deployment: pod.Deployment, NodeID: pod.NodeID,
			Status: pod.Status, HostPort: pod.HostPort, RestartCount: pod.RestartCount,
		})
	}

	sort.Slice(views, func(i, j int) bool {
		if views[i].Deployment != views[j].Deployment {
			return views[i].Deployment < views[j].Deployment
		}
		return views[i].PodID < views[j].PodID
	})
	return views, nil
}

// nodeViews reads every registered node and whether its lease is present.
func (s *Server) nodeViews(ctx context.Context, podsByNode map[string]int) ([]NodeView, error) {
	registered, err := s.client.SetMembers(ctx, schema.NodeRegistryKey())
	if err != nil {
		return nil, fmt.Errorf("read node registry: %w", err)
	}
	sort.Strings(registered)

	views := make([]NodeView, 0, len(registered))
	for _, nodeID := range registered {
		fields, err := s.client.HashGet(ctx, schema.NodeCapacityKey(nodeID))
		if err != nil || len(fields) == 0 {
			continue
		}
		capacity, err := schema.MapToNodeCapacity(fields)
		if err != nil {
			continue
		}
		ready, err := s.client.Exists(ctx, schema.NodeStatusKey(nodeID))
		if err != nil {
			ready = false
		}
		views = append(views, NodeView{
			NodeID: nodeID, Ready: ready,
			TotalCPU: capacity.TotalCPU, AllocatedCPU: capacity.AllocatedCPU,
			TotalMem: capacity.TotalMem, AllocatedMem: capacity.AllocatedMem,
			PodCount: podsByNode[nodeID],
		})
	}
	return views, nil
}

// deploymentViews reads every deployment with its observed utilization.
func (s *Server) deploymentViews(ctx context.Context, ready map[string]int) ([]DeploymentView, error) {
	deps, err := s.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}

	views := make([]DeploymentView, 0, len(deps))
	for _, dep := range deps {
		views = append(views, DeploymentView{
			Name: dep.Name, DesiredReplicas: dep.DesiredReplicas,
			ReadyReplicas: ready[dep.Name],
			MinReplicas:   dep.MinReplicas, MaxReplicas: dep.MaxReplicas,
			TargetCPUPercent: dep.TargetCPUPercent,
			ObservedCPU:      s.observedCPU(ctx, dep.Name),
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views, nil
}

// observedCPU averages the recent telemetry of a deployment's pods.
//
// This mirrors what the autoscaler computes rather than reusing it: the
// autoscaler's version is inside a Lua script precisely so its read and write
// cannot be separated, and calling it here would apply a scaling decision as a
// side effect of drawing a page.
func (s *Server) observedCPU(ctx context.Context, deployment string) int {
	keys, err := s.client.ScanKeys(ctx, fmt.Sprintf("telemetry:%s:*:cpu", deployment))
	if err != nil {
		return 0
	}

	total, pods := 0, 0
	for _, key := range keys {
		samples, err := s.client.ListRange(ctx, key, -3, -1)
		if err != nil || len(samples) == 0 {
			continue
		}
		sum, count := 0, 0
		for _, raw := range samples {
			v, err := strconv.Atoi(raw)
			if err != nil {
				continue
			}
			sum += v
			count++
		}
		if count == 0 {
			continue
		}
		total += sum / count
		pods++
	}
	if pods == 0 {
		return 0
	}
	return total / pods
}

// recentLogs returns log entries newer than sinceLogID, oldest first, along with
// the newest id seen.
func (s *Server) recentLogs(ctx context.Context, sinceLogID string, count int64) ([]LogEntry, string, error) {
	entries, err := s.client.StreamReadReverse(ctx, schema.LogStreamKey(), count)
	if err != nil {
		return nil, "", err
	}
	if len(entries) == 0 {
		return nil, "", nil
	}

	// The read returns newest first; the panel appends chronologically.
	newest := entries[0].ID
	logs := make([]LogEntry, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if sinceLogID != "" && !streamIDNewer(e.ID, sinceLogID) {
			continue
		}
		logs = append(logs, LogEntry{
			ID:         e.ID,
			Timestamp:  e.Fields["timestamp"],
			Level:      e.Fields["level"],
			Component:  e.Fields["component"],
			EventType:  e.Fields["event_type"],
			Message:    e.Fields["message"],
			Deployment: e.Fields["deployment_id"],
			PodID:      e.Fields["pod_id"],
			NodeID:     e.Fields["node_id"],
		})
	}
	return logs, newest, nil
}

// streamIDNewer compares two Redis stream ids.
//
// The parts are compared numerically, not as strings: ids are "ms-seq", and a
// string comparison would order "10-0" before "9-0" and silently drop entries.
func streamIDNewer(a, b string) bool {
	aMS, aSeq := splitStreamID(a)
	bMS, bSeq := splitStreamID(b)
	if aMS != bMS {
		return aMS > bMS
	}
	return aSeq > bSeq
}

// splitStreamID parses "ms-seq" into its two numbers.
func splitStreamID(id string) (uint64, uint64) {
	ms, seq, found := strings.Cut(id, "-")
	msVal, _ := strconv.ParseUint(ms, 10, 64)
	if !found {
		return msVal, 0
	}
	seqVal, _ := strconv.ParseUint(seq, 10, 64)
	return msVal, seqVal
}
