//go:build linux

package gostage

import (
	"runtime"
	"syscall"
)

// setPdeathsig sets SIGKILL as the parent death signal on the child process.
// The calling goroutine is locked to its OS thread because Linux's prctl(PR_SET_PDEATHSIG)
// applies to the thread, not the process, and Go's scheduler may migrate goroutines.
func setPdeathsig(attr *syscall.SysProcAttr) {
	runtime.LockOSThread()
	attr.Pdeathsig = syscall.SIGKILL
}
