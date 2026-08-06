// Package supervisor runs and watches the workload processes assigned to a
// node. It owns process spawning, durable process identity, and the startup
// reconciliation that makes an agent restart safe.
package supervisor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// statStartTimeField is the 1-based index of `starttime` in /proc/[pid]/stat,
// counted from the field after the comm field. See proc(5).
const statStartTimeField = 22

// ProcessStartTime reads a process's start time in clock ticks since boot.
//
// This is the defense against PID reuse. A PID alone is not an identity: the
// kernel recycles PIDs, so a recorded PID may later belong to an unrelated
// process. Start time is fixed for the life of a process, so the pair
// (pid, start_time) identifies it uniquely on a running machine.
//
// It returns 0 with an error if the process does not exist.
func ProcessStartTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}

	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, fmt.Errorf("read stat for pid %d: %w", pid, err)
	}

	// The comm field (field 2) is wrapped in parentheses and may itself
	// contain spaces and parentheses, so fields cannot simply be split on
	// whitespace. Everything after the FINAL ')' is safe to split.
	line := string(data)
	close := strings.LastIndexByte(line, ')')
	if close < 0 || close+2 >= len(line) {
		return 0, fmt.Errorf("malformed stat for pid %d", pid)
	}

	// Fields after comm start at field 3, so starttime (field 22) sits at
	// index 22-3 = 19 of the remainder.
	rest := strings.Fields(line[close+2:])
	idx := statStartTimeField - 3
	if idx >= len(rest) {
		return 0, fmt.Errorf("stat for pid %d has %d fields after comm, need %d", pid, len(rest), idx+1)
	}

	ticks, err := strconv.ParseUint(rest[idx], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse start time for pid %d: %w", pid, err)
	}
	return ticks, nil
}

// PIDAlive reports whether a PID currently exists.
//
// Signal 0 performs the kernel's permission and existence checks without
// delivering anything. An EPERM answer means the process exists but is owned
// by another user, which still counts as alive.
func PIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return err == syscall.EPERM
}

// IsSameProcess reports whether pid is alive AND is the same process that was
// recorded with wantStartTime.
//
// A recorded start time of 0 means the marker was never captured (an older
// record, or a crash between spawn and the identity write). Identity then
// cannot be proven, so this reports false: the caller must treat the pod as
// needing a restart rather than adopting a process it cannot verify. Adopting
// wrongly would leave a stranger's process supervised as if it were the pod.
func IsSameProcess(pid int, wantStartTime uint64) bool {
	if pid <= 0 || wantStartTime == 0 {
		return false
	}
	if !PIDAlive(pid) {
		return false
	}
	got, err := ProcessStartTime(pid)
	if err != nil {
		return false
	}
	return got == wantStartTime
}
