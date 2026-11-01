package supervisor

// IsSameProcess reports whether pid is alive AND is the same process that was
// recorded with wantStartTime.
//
// A recorded start time of 0 means the marker was never captured (an older
// record, or a crash between spawn and the identity write). Identity then
// cannot be proven, so this reports false.
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

