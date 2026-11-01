//go:build windows

package supervisor

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"mini-k8s/internal/logging"
	"mini-k8s/internal/redisclient"
	"mini-k8s/internal/schema"
)

// Environment variables handed to every workload process.
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
	portScanLimit = 512
)

// stopGrace is how long a process is given to exit before it is killed.
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
		return fmt.Errorf("pod %s/%s no longer exists", deployment, podID)
	}
	pod, err := schema.MapToPod(podFields)
	if err != nil {
		return fmt.Errorf("pod %s/%s is malformed: %w", deployment, podID, err)
	}

	if IsSameProcess(pod.PID, pod.PIDStartTime) {
		s.logger.Info(ctx, "pod_already_running",
			fmt.Sprintf("pid %d is already serving this pod, not spawning a duplicate", pod.PID),
			logging.DeploymentID(deployment), logging.PodID(podID), logging.NodeID(s.nodeID))
		return nil
	}

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

	// Minimal environment — do not inherit agent credentials.
	cmd.Env = []string{
		"PATH=" + envOrDefault("PATH", `C:\Windows\system32;C:\Windows`),
		"HOME=" + envOrDefault("USERPROFILE", os.TempDir()),
		PodIDEnv + "=" + pod.PodID,
		DeploymentEnv + "=" + dep.Name,
		NodeIDEnv + "=" + s.nodeID,
		PortEnv + "=" + strconv.Itoa(hostPort),
	}

	// Windows has no Setpgid; processes run in their own console by default.
	// cmd.SysProcAttr is left nil — acceptable on Windows.

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

	go func() { _ = cmd.Wait() }()

	// On Windows ProcessStartTime returns a sentinel 1 for live processes.
	startTime, err := ProcessStartTime(pid)
	if err != nil {
		return fmt.Errorf("process %d exited immediately after start: %w", pid, err)
	}

	if err := s.client.HashSet(ctx, podKey, map[string]interface{}{
		schema.FieldPodPID:          pid,
		schema.FieldPodPIDStartTime: startTime,
		schema.FieldPodHostPort:     hostPort,
		schema.FieldPodStartedAt:    strconv.FormatInt(time.Now().Unix(), 10),
		schema.FieldPodStatus:       schema.StatusRunning,
	}); err != nil {
		s.terminate(cmd.Process)
		return fmt.Errorf("record identity for pid %d: %w", pid, err)
	}

	s.logger.Info(ctx, "pod_started",
		fmt.Sprintf("spawned %s as pid %d on port %d", dep.ExecPath, pid, hostPort),
		logging.DeploymentID(dep.Name), logging.PodID(pod.PodID), logging.NodeID(s.nodeID))
	return nil
}

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
func (s *Supervisor) Stop(ctx context.Context, deployment, podID string) error {
	podKey := schema.PodKey(deployment, podID)

	fields, err := s.client.HashGet(ctx, podKey)
	if err != nil {
		return fmt.Errorf("read pod %s: %w", podID, err)
	}
	if len(fields) == 0 {
		return nil
	}
	pod, err := schema.MapToPod(fields)
	if err != nil {
		return fmt.Errorf("pod %s/%s is malformed: %w", deployment, podID, err)
	}

	if IsSameProcess(pod.PID, pod.PIDStartTime) {
		p, err := os.FindProcess(pod.PID)
		if err == nil {
			s.terminate(p)
		}
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

// terminate kills a process on Windows using os.Process.Kill with a grace period.
func (s *Supervisor) terminate(p *os.Process) {
	if p == nil {
		return
	}
	// Windows has no SIGTERM equivalent via syscall from another process;
	// we go straight to Kill after the grace window.
	done := make(chan struct{})
	go func() {
		_, _ = p.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(stopGrace):
		_ = p.Kill()
	}
}

// allocateHostPort picks a free port for a pod on this node.
func (s *Supervisor) allocateHostPort(ctx context.Context, preferred int) (int, error) {
	claimed, err := s.claimedPorts(ctx)
	if err != nil {
		return 0, err
	}

	candidates := make([]int, 0, portScanLimit+1)
	if preferred >= minHostPort && preferred <= maxHostPort {
		candidates = append(candidates, preferred)
	} else if preferred > 0 {
		candidates = append(candidates, minHostPort+(preferred%1000))
	}
	for i := 0; i < portScanLimit; i++ {
		candidates = append(candidates, minHostPort+i)
	}

	for _, port := range candidates {
		if port < minHostPort || port > maxHostPort || claimed[port] {
			continue
		}
		if !portFree(port) {
			continue
		}
		return port, nil
	}
	return 0, fmt.Errorf("no free host port in range %d-%d", minHostPort, minHostPort+portScanLimit)
}

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

func portFree(port int) bool {
	listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}
