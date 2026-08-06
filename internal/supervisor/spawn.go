package supervisor

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Environment variables handed to every workload process.
//
// PodIDEnv is also the recovery marker: it lets the agent find a process it
// spawned even if the record of its PID was lost, which is what prevents a
// crash mid-spawn from leaving an orphan behind.
const (
	PodIDEnv      = "MINIK8S_POD_ID"
	DeploymentEnv = "MINIK8S_DEPLOYMENT"
	NodeIDEnv     = "MINIK8S_NODE_ID"
	PortEnv       = "PORT"
)

// Port allocation bounds for per-pod host ports.
const (
	minHostPort   = 20000
	maxHostPort   = 60000
	portScanLimit = 512 // how many candidates to try before giving up
)

// stopGrace is how long a process is given to exit on SIGTERM before it is
// killed outright.
const stopGrace = 5 * time.Second

// Supervisor spawns and stops workload processes for one node.
type Supervisor struct {
	client *redisclient.Client
	logger *logging.Logger
	nodeID string
}

// NewSupervisor builds a Supervisor bound to a node.
func NewSupervisor(client *redisclient.Client, logger *logging.Logger, nodeID string) *Supervisor {
	return &Supervisor{client: client, logger: logger, nodeID: nodeID}
}

// Spawn launches the workload process for a pod and records its identity.
//
// The pod and deployment are re-read from Redis rather than taken from the
// caller, so a command that waited in the queue while the spec changed cannot
// apply stale parameters.
func (s *Supervisor) Spawn(ctx context.Context, deployment, podID string) error {
	depFields, err := s.client.HashGet(ctx, schema.DeploymentKey(deployment))
	if err != nil {
		return fmt.Errorf("read deployment %s: %w", deployment, err)
	}
	if len(depFields) == 0 {
		return fmt.Errorf("deployment %s no longer exists", deployment)
	}
	dep, err := schema.MapToDeployment(depFields)
	if err != nil {
		return fmt.Errorf("deployment %s is malformed: %w", deployment, err)
	}

	podKey := schema.PodKey(deployment, podID)
	podFields, err := s.client.HashGet(ctx, podKey)
	if err != nil {
		return fmt.Errorf("read pod %s: %w", podID, err)
	}
	if len(podFields) == 0 {
		// The pod was deleted while its start command sat in the queue.
		// Spawning now would create a process nothing owns.
		return fmt.Errorf("pod %s/%s no longer exists", deployment, podID)
	}
	pod, err := schema.MapToPod(podFields)
	if err != nil {
		return fmt.Errorf("pod %s/%s is malformed: %w", deployment, podID, err)
	}

	// A pod already backed by a verifiable live process must never be spawned
	// twice; that is the duplicate this whole identity scheme exists to stop.
	if IsSameProcess(pod.PID, pod.PIDStartTime) {
		s.logger.Info(ctx, "pod_already_running",
			fmt.Sprintf("pid %d is already serving this pod, not spawning a duplicate", pod.PID),
			logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
		return nil
	}

	// Reuse the port already assigned to this pod so a restart keeps the same
	// address; ingress upstreams and in-flight clients depend on it.
	hostPort := pod.HostPort
	if hostPort == 0 {
		hostPort, err = s.allocateHostPort(ctx, dep.Port)
		if err != nil {
			return fmt.Errorf("allocate host port for %s/%s: %w", deployment, podID, err)
		}
	}

	if err := s.markFailedIfSpawnFails(ctx, podKey, deployment, podID, func() error {
		return s.launch(ctx, dep, pod, podKey, hostPort)
	}); err != nil {
		return err
	}
	return nil
}

// launch starts the process and writes its durable identity.
func (s *Supervisor) launch(ctx context.Context, dep *schema.DeploymentSpec, pod *schema.PodSpec, podKey string, hostPort int) error {
	if _, err := os.Stat(dep.ExecPath); err != nil {
		return fmt.Errorf("exec path %s is not usable: %w", dep.ExecPath, err)
	}

	cmd := exec.Command(dep.ExecPath)

	// A deliberately minimal environment: the workload is arbitrary
	// user-supplied code, so inheriting the agent's full environment would hand
	// it every credential the agent happens to hold (its Redis address, tokens
	// in the operator's shell, and so on). Only PATH and HOME are carried over,
	// since most programs need them to run at all.
	//
	// The pod identity travels here too, which lets a process be traced back to
	// its pod by scanning /proc even if the record of its PID was lost.
	cmd.Env = []string{
		"PATH=" + envOrDefault("PATH", "/usr/local/bin:/usr/bin:/bin"),
		"HOME=" + envOrDefault("HOME", "/tmp"),
		PodIDEnv + "=" + pod.PodID,
		DeploymentEnv + "=" + dep.Name,
		NodeIDEnv + "=" + s.nodeID,
		PortEnv + "=" + strconv.Itoa(hostPort),
	}

	// Its own process group, so stopping the pod reaches the whole tree
	// rather than only the direct child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Detach stdio: inheriting the agent's pipes would block the workload if
	// nothing drains them, and would tangle its output with the agent's logs.
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer devNull.Close()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, devNull, devNull

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", dep.ExecPath, err)
	}

	pid := cmd.Process.Pid

	// Reap the child when it exits. Without this the process would linger as a
	// zombie, which still holds its PID and would make liveness checks report
	// a dead workload as alive.
	go func() { _ = cmd.Wait() }()

	// Capture identity immediately. A start time that cannot be read means the
	// process already exited, which is a spawn failure rather than a success.
	startTime, err := ProcessStartTime(pid)
	if err != nil {
		return fmt.Errorf("process %d exited immediately after start: %w", pid, err)
	}

	// Write identity before flipping to Running, so a crash in between leaves a
	// record that can be verified or reclaimed rather than an untraceable process.
	if err := s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodPID:          pid,
		schema.FieldPodPIDStartTime: startTime,
		schema.FieldPodHostPort:     hostPort,
		schema.FieldPodStartedAt:    strconv.FormatInt(time.Now().Unix(), 10),
		schema.FieldPodStatus:       schema.StatusRunning,
	}); err != nil {
		// The process is running but unrecorded. Stop it rather than leaking a
		// process no future reconcile could account for.
		s.terminate(pid)
		return fmt.Errorf("record identity for pid %d: %w", pid, err)
	}

	s.logger.Info(ctx, "pod_started",
		fmt.Sprintf("spawned %s as pid %d on port %d", dep.ExecPath, pid, hostPort),
		logging.DeploymentID(dep.Name), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
	return nil
}

// markFailedIfSpawnFails records a spawn failure on the pod so the control
// plane can see why a pod is not running, instead of it sitting in Pending
// with no explanation.
func (s *Supervisor) markFailedIfSpawnFails(ctx context.Context, podKey, deployment, podID string, spawn func() error) error {
	err := spawn()
	if err == nil {
		return nil
	}

	if setErr := s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodStatus: schema.StatusFailed,
	}); setErr != nil {
		s.logger.Error(ctx, "pod_status_write_failed",
			fmt.Sprintf("could not mark pod Failed after spawn error: %v", setErr),
			logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
	}

	s.logger.Error(ctx, "pod_spawn_failed", err.Error(),
		logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
	return err
}

// Stop terminates a pod's process and clears its recorded identity.
//
// Identity is cleared even when signalling fails, so a stopped pod is never
// left pointing at a PID that may later be recycled by an unrelated process.
func (s *Supervisor) Stop(ctx context.Context, deployment, podID string) error {
	podKey := schema.PodKey(deployment, podID)

	fields, err := s.client.HashGet(ctx, podKey)
	if err != nil {
		return fmt.Errorf("read pod %s: %w", podID, err)
	}
	if len(fields) == 0 {
		return nil // already gone
	}
	pod, err := schema.MapToPod(fields)
	if err != nil {
		return fmt.Errorf("pod %s/%s is malformed: %w", deployment, podID, err)
	}

	if IsSameProcess(pod.PID, pod.PIDStartTime) {
		s.terminate(pod.PID)
		s.logger.Info(ctx, "pod_stopped",
			fmt.Sprintf("terminated pid %d", pod.PID),
			logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
	}

	if err := s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodPID:          0,
		schema.FieldPodPIDStartTime: 0,
	}); err != nil {
		return fmt.Errorf("clear identity for %s/%s: %w", deployment, podID, err)
	}
	return nil
}

// terminate asks a process group to exit, then kills it if it does not.
//
// The negative PID targets the whole process group, so a workload that spawned
// children does not leave them running.
func (s *Supervisor) terminate(pid int) {
	if pid <= 0 {
		return
	}

	syscall.Kill(-pid, syscall.SIGTERM)

	deadline := time.Now().Add(stopGrace)
	for time.Now().Before(deadline) {
		if !PIDAlive(pid) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Ignoring SIGTERM is not a reason to leak a process holding a port.
	syscall.Kill(-pid, syscall.SIGKILL)
}

// allocateHostPort picks a port for a pod on this node.
//
// Pods here are plain processes sharing the node's network namespace, unlike
// real Kubernetes where each pod gets its own. Two replicas of one deployment
// on the same node would therefore both try to bind the deployment's declared
// port and the second would fail, so each pod gets a distinct host port.
//
// The preferred port is tried first, so a single replica per node keeps the
// port the user declared.
func (s *Supervisor) allocateHostPort(ctx context.Context, preferred int) (int, error) {
	claimed, err := s.claimedPorts(ctx)
	if err != nil {
		return 0, err
	}

	candidates := make([]int, 0, portScanLimit+1)
	if preferred >= minHostPort && preferred <= maxHostPort {
		candidates = append(candidates, preferred)
	} else if preferred > 0 {
		// A privileged or out-of-range declared port cannot be used directly,
		// so fall through to the ephemeral range.
		candidates = append(candidates, minHostPort+(preferred%1000))
	}
	for i := 0; i < portScanLimit; i++ {
		candidates = append(candidates, minHostPort+i)
	}

	for _, port := range candidates {
		if port < minHostPort || port > maxHostPort || claimed[port] {
			continue
		}
		// Redis says the port is unclaimed; the kernel decides whether it is
		// actually free, since something outside the cluster may hold it.
		if !portFree(port) {
			continue
		}
		return port, nil
	}
	return 0, fmt.Errorf("no free host port in range %d-%d", minHostPort, minHostPort+portScanLimit)
}

// claimedPorts returns host ports already assigned to pods on this node.
func (s *Supervisor) claimedPorts(ctx context.Context) (map[int]bool, error) {
	keys, err := s.client.ScanKeys(ctx, schema.PodKeyPattern())
	if err != nil {
		return nil, fmt.Errorf("scan pods: %w", err)
	}

	claimed := make(map[int]bool, len(keys))
	for _, key := range keys {
		fields, err := s.client.HashGet(ctx, key)
		if err != nil || len(fields) == 0 {
			continue
		}
		if fields[schema.FieldPodNodeID] != s.nodeID {
			continue
		}
		if port, convErr := strconv.Atoi(fields[schema.FieldPodHostPort]); convErr == nil && port > 0 {
			claimed[port] = true
		}
	}
	return claimed, nil
}

// envOrDefault reads an environment variable, falling back when it is unset.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// portFree reports whether a TCP port can currently be bound.
//
// This is advisory: the port could be taken between this check and the
// workload's own bind. Losing that race surfaces as a spawn failure and a
// retry, which is why the check is a filter rather than a guarantee.
func portFree(port int) bool {
	listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}
