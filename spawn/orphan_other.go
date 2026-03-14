//go:build !linux

package spawn

import "syscall"

// setPdeathsig is a no-op on non-Linux platforms.
// Orphan detection relies on lifeline pipe and PID polling instead.
func setPdeathsig(_ *syscall.SysProcAttr) {}
