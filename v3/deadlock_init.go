package gostage

import (
	"fmt"
	"runtime"
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

func init() {
	deadlock.Opts.DeadlockTimeout = 2 * time.Second
	deadlock.Opts.OnPotentialDeadlock = func() {
		fmt.Println("Potential deadlock detected")
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		fmt.Printf("Stack trace:\n%s\n", string(buf[:n]))
	}
}
