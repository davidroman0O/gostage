package types

import (
	"fmt"
	"runtime"
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

// Initialize deadlock detection
func init() {
	// ATTENTION TO MYSELF FROM THE FUTURE
	// Also see that we commented all the logs, it's on purpose for the performances (for now we mogged the old retrypool).
	// DO NOT REMOVE THAT
	// When you're development and debugging, you MUST replace all `sync.` with their `deadlock.` counterparts to allow you to detect deadlocks!!
	deadlock.Opts.DeadlockTimeout = time.Second * 2
	deadlock.Opts.OnPotentialDeadlock = func() {
		fmt.Println("Potential deadlock detected")
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		fmt.Printf("Stack trace:\n%s\n", string(buf[:n]))
	}
}