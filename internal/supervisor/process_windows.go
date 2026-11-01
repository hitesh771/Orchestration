//go:build windows

package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// ProcessStartTime returns a stable sentinel for a live process on Windows.
//
// Windows does not expose /proc/[pid]/stat. We verify the process is alive
// and return a fixed non-zero sentinel (1). This means PID-reuse detection
// across agent restarts is not supported on Windows — a known limitation.
func ProcessStartTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	if !PIDAlive(pid) {
		return 0, fmt.Errorf("process %d is not alive", pid)
	}
	return 1, nil
}

// PIDAlive reports whether a process with the given PID currently exists on Windows.
//
// We use `tasklist /FI "PID eq <pid>" /NH /FO CSV` which is available on all
// Windows versions without requiring elevated privileges.
func PIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	out, err := exec.Command(
		"tasklist",
		"/FI", "PID eq "+strconv.Itoa(pid),
		"/NH", "/FO", "CSV",
	).Output()
	if err != nil {
		return false
	}
	// tasklist outputs "INFO: No tasks..." when not found, or CSV rows when found.
	output := strings.TrimSpace(string(out))
	return !strings.HasPrefix(output, "INFO:") && len(output) > 0
}

// envOrDefault reads an environment variable, falling back when it is unset.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
