package workerhost

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage/v2/runner"
)

func newSlot(id string, runner *runner.Runner, h *host) *slot {
	return &slot{
		id:     id,
		runner: runner,
		host:   h,
		jobCh:  make(chan Job),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (s *slot) run() {
	defer close(s.done)
	defer s.host.wg.Done()

	for {
		select {
		case job := <-s.jobCh:
			s.handleJob(job)
			if s.stopRequested.Load() {
				s.host.handleSlotRemovalBusy(s)
				return
			}
			select {
			case s.host.idleCh <- s:
			case <-s.host.stopCh:
				s.host.handleSlotRemovalBusy(s)
				return
			case <-s.stopCh:
				s.host.handleSlotRemovalBusy(s)
				return
			}
		case <-s.stopCh:
			s.host.handleSlotRemovalBusy(s)
			return
		case <-s.host.stopCh:
			s.host.handleSlotRemovalBusy(s)
			return
		}
	}
}

func (s *slot) handleJob(job Job) {
	if job.Workflow == nil {
		return
	}

	s.host.onJobStarted(s, job)

	ctx := job.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	s.setCancel(cancel)
	defer func() {
		cancel()
		s.setCancel(nil)
	}()

	var res runner.RunResult
	var panicErr interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = r
			}
		}()
		res = s.runner.Run(job.Workflow, runner.RunOptions{InitialStore: job.InitialStore, Context: ctx})
	}()

	if panicErr != nil {
		err := panicError(panicErr)
		s.host.onJobFinished(s, job, runner.RunResult{Success: false, Error: err}, EventFailed, err)
		return
	}

	if ctx.Err() == context.Canceled {
		err := ctx.Err()
		res.Success = false
		res.Error = err
		s.host.onJobFinished(s, job, res, EventCancelled, err)
		return
	}

	if res.Success {
		s.host.onJobFinished(s, job, res, EventCompleted, nil)
		return
	}

	if res.Error == nil {
		res.Error = ErrHostStopped
	}
	s.host.onJobFinished(s, job, res, EventFailed, res.Error)
}

func (s *slot) reserve() bool {
	if s.stopRequested.Load() {
		return false
	}
	return s.reserved.CompareAndSwap(false, true)
}

func (s *slot) releaseReservation() bool {
	return s.reserved.CompareAndSwap(true, false)
}

func (s *slot) markStarted() {
	s.reserved.Store(false)
}

func (s *slot) requestStop() {
	if s.stopRequested.CompareAndSwap(false, true) {
		s.cancelJob()
		s.stopOnce.Do(func() { close(s.stopCh) })
	}
}

func (s *slot) cancelJob() {
	s.cancelMu.Lock()
	cancel := s.cancel
	s.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *slot) setCancel(cancel context.CancelFunc) {
	s.cancelMu.Lock()
	s.cancel = cancel
	s.cancelMu.Unlock()
}

func panicError(val interface{}) error {
	if err, ok := val.(error); ok {
		return err
	}
	return fmt.Errorf("workerhost: slot panic: %v", val)
}
