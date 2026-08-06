package supervisor

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// helperBinary is the compiled test workload, shared by every test.
var helperBinary string

// TestMain compiles the test workload once. A real long-running binary is
// needed because most system executables exit immediately with no arguments,
// which would make supervision untestable.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "minik8s-supervisor-test")
	if err != nil {
		panic("create temp dir: " + err.Error())
	}
	defer os.RemoveAll(dir)

	helperBinary = filepath.Join(dir, "workload")
	build := exec.Command("go", "build", "-o", helperBinary, "./testdata/workload")
	if out, err := build.CombinedOutput(); err != nil {
		panic("build test workload: " + err.Error() + "\n" + string(out))
	}

	os.Exit(m.Run())
}

// startHelper launches the test workload and registers its cleanup.
func startHelper(t *testing.T, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(helperBinary)
	cmd.Env = append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	})
	return cmd
}

// waitGone waits for a PID to disappear, so tests do not race process teardown.
func waitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !PIDAlive(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pid %d still alive after waiting", pid)
}

// waitForMarker polls until a process's pod marker becomes readable, which
// happens once exec has completed.
func waitForMarker(t *testing.T, pid int) (podID, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p, n, ok := readPodMarker(pid); ok {
			return p, n
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pod marker never became readable for pid %d", pid)
	return "", ""
}

func TestProcessStartTimeIsStableAcrossReads(t *testing.T) {
	cmd := startHelper(t)
	pid := cmd.Process.Pid

	first, err := ProcessStartTime(pid)
	if err != nil {
		t.Fatalf("ProcessStartTime: %v", err)
	}
	if first == 0 {
		t.Fatal("start time should never be 0 for a live process")
	}

	// The value must be fixed for the life of the process, which is what makes
	// it usable as an identity marker.
	time.Sleep(100 * time.Millisecond)
	second, err := ProcessStartTime(pid)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if first != second {
		t.Errorf("start time changed between reads: %d then %d", first, second)
	}
}

func TestProcessStartTimeFailsForDeadOrInvalidPID(t *testing.T) {
	cmd := startHelper(t)
	pid := cmd.Process.Pid
	syscall.Kill(-pid, syscall.SIGKILL)
	waitGone(t, pid)

	if _, err := ProcessStartTime(pid); err == nil {
		t.Error("reading start time of a dead process should fail")
	}
	for _, bad := range []int{0, -1, -99999} {
		if _, err := ProcessStartTime(bad); err == nil {
			t.Errorf("pid %d should be rejected", bad)
		}
	}
}

// The parser must not split on whitespace, because the comm field can contain
// spaces and parentheses. Reading the agent's own stat exercises the real
// format end to end.
func TestProcessStartTimeParsesOwnStat(t *testing.T) {
	ticks, err := ProcessStartTime(os.Getpid())
	if err != nil {
		t.Fatalf("reading own start time: %v", err)
	}
	if ticks == 0 {
		t.Error("own start time should be non-zero")
	}
}

func TestPIDAliveTracksProcessLifetime(t *testing.T) {
	cmd := startHelper(t)
	pid := cmd.Process.Pid

	if !PIDAlive(pid) {
		t.Fatal("helper should be alive right after start")
	}

	syscall.Kill(-pid, syscall.SIGKILL)
	waitGone(t, pid)

	if PIDAlive(pid) {
		t.Error("PIDAlive should report false once the process is reaped")
	}
	for _, bad := range []int{0, -1} {
		if PIDAlive(bad) {
			t.Errorf("pid %d should not be reported alive", bad)
		}
	}
}

func TestIsSameProcessMatchesOnlyTheRecordedProcess(t *testing.T) {
	cmd := startHelper(t)
	pid := cmd.Process.Pid
	startTime, err := ProcessStartTime(pid)
	if err != nil {
		t.Fatalf("ProcessStartTime: %v", err)
	}

	if !IsSameProcess(pid, startTime) {
		t.Error("a live process with its own recorded start time should match")
	}

	// A mismatched start time is exactly the PID-reuse case: same PID, but a
	// different process now holds it.
	if IsSameProcess(pid, startTime+1) {
		t.Error("a different start time must not match; this is the PID-reuse defense")
	}

	// An unknown marker cannot prove identity, so it must not be trusted.
	if IsSameProcess(pid, 0) {
		t.Error("start time 0 means unverifiable and must never match")
	}

	syscall.Kill(-pid, syscall.SIGKILL)
	waitGone(t, pid)
	if IsSameProcess(pid, startTime) {
		t.Error("a dead process must not match")
	}
}

func TestReadPodMarkerRecoversIdentityFromProcessEnvironment(t *testing.T) {
	cmd := startHelper(t,
		PodIDEnv+"=pod-xyz",
		NodeIDEnv+"=node-7",
		DeploymentEnv+"=payment-api",
	)

	// The environment is not populated until exec completes, so a process
	// sampled immediately after start reads back an empty environ.
	podID, nodeID := waitForMarker(t, cmd.Process.Pid)
	if podID != "pod-xyz" {
		t.Errorf("pod id = %q, want pod-xyz", podID)
	}
	if nodeID != "node-7" {
		t.Errorf("node id = %q, want node-7", nodeID)
	}
}

// A process without the marker must not be mistaken for a pod, or the orphan
// sweep would kill unrelated processes.
func TestReadPodMarkerRejectsUnmarkedProcess(t *testing.T) {
	cmd := startHelper(t)
	// Sampled repeatedly across the exec window: an unmarked process must never
	// become identifiable as a pod, not merely be unreadable at first.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, _, ok := readPodMarker(cmd.Process.Pid); ok {
			t.Fatal("an unmarked process must not be reported as a pod")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
