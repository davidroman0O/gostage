package spawn

import (
	"context"
	"os"
	"time"
)

// orphanWatcher monitors for parent death using multiple mechanisms.
// When any mechanism detects parent death, the provided cancel function is called.
type orphanWatcher struct {
	cancel context.CancelFunc
}

// startOrphanWatcher starts all orphan detection mechanisms.
// Call the returned stop function to clean up when no longer needed.
func startOrphanWatcher(ctx context.Context, cancel context.CancelFunc, lifelineFd *os.File) func() {
	w := &orphanWatcher{cancel: cancel}

	// Mechanism 1: Lifeline pipe -- EOF means parent died
	if lifelineFd != nil {
		go w.watchLifeline(ctx, lifelineFd)
	}

	// Mechanism 2: PID polling -- re-parented to PID 1 means parent died
	go w.watchPID(ctx)

	return func() {
		if lifelineFd != nil {
			lifelineFd.Close()
		}
	}
}

// watchLifeline blocks on reading the lifeline pipe's read end.
// When the parent dies, the OS closes the write end and Read returns.
func (w *orphanWatcher) watchLifeline(ctx context.Context, fd *os.File) {
	buf := make([]byte, 1)
	for {
		// Read blocks until parent closes write end (parent death) or context cancels
		_, err := fd.Read(buf)
		if err != nil {
			// EOF or read error -- parent is dead
			select {
			case <-ctx.Done():
				return // already cancelled
			default:
				w.cancel()
				return
			}
		}
	}
}

// watchPID polls os.Getppid to detect re-parenting (parent death on Unix).
func (w *orphanWatcher) watchPID(ctx context.Context) {
	originalPPID := os.Getppid()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if os.Getppid() != originalPPID {
				w.cancel()
				return
			}
		}
	}
}
