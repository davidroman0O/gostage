package spawner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/davidroman0O/gostage/v3/shared/child"
	"github.com/davidroman0O/gostage/v3/internal/infrastructure/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/foundation/clock"
	"github.com/davidroman0O/gostage/v3/shared/concurrency"
	"github.com/davidroman0O/gostage/v3/internal/foundation/locks"
)

// Reporter is defined in interfaces.go.
// LoggerReporter provides a function-based implementation.

// LoggerReporter adapts a function to Reporter.
type LoggerReporter func(diagnostics.Event)

// Report invokes the underlying function with the diagnostic event.
func (fn LoggerReporter) Report(evt diagnostics.Event) {
	if fn != nil {
		fn(evt)
	}
}

// Config controls process spawner behaviour.
type Config struct {
	BinaryPath           string
	Args                 []string
	Env                  []string
	WorkingDir           string
	MaxRestarts          int
	RestartBackoff       time.Duration
	Reporter             Reporter
	ShutdownGrace        time.Duration
	DisableOutputCapture bool
}

// LaunchConfig specifies runtime parameters for a child process.
type LaunchConfig struct {
	Address   string
	AuthToken string
	ChildType string
	Metadata  map[string]string
	TLS       child.TLSConfig
	Pools     []child.PoolSpec
	ExtraArgs []string
	ExtraEnv  []string
}

// ProcessSpawner launches child processes using the host binary.
// It implements the Spawner interface defined in interfaces.go.
type ProcessSpawner struct {
	cfg      Config
	mu       *locks.OrderedRWMutex
	executor *concurrency.DeterministicExecutor
	clock    clock.Clock
}

// NewProcessSpawner constructs a spawner with the supplied config.
func NewProcessSpawner(cfg Config) *ProcessSpawner {
	if cfg.RestartBackoff <= 0 {
		cfg.RestartBackoff = 2 * time.Second
	}
	if cfg.MaxRestarts < 0 {
		cfg.MaxRestarts = 0
	}
	if cfg.Reporter == nil {
		cfg.Reporter = LoggerReporter(func(_ diagnostics.Event) {})
	}
	c := clock.DefaultClock()
	return &ProcessSpawner{
		cfg:      cfg,
		mu:       locks.NewOrderedRWMutex(locks.LockOrderRegistry),
		executor: concurrency.NewDeterministicExecutor(c),
		clock:    c,
	}
}

// SetReporter updates the diagnostics reporter used for child processes.
func (s *ProcessSpawner) SetReporter(rep Reporter) {
	if rep == nil {
		rep = LoggerReporter(func(_ diagnostics.Event) {})
	}
	s.mu.Lock()
	s.cfg.Reporter = rep
	s.mu.Unlock()
}

// Reporter returns the currently configured diagnostics reporter.
func (s *ProcessSpawner) Reporter() Reporter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.Reporter
}

// Launch starts a child process with the provided launch configuration and returns a handle.
func (s *ProcessSpawner) Launch(ctx context.Context, launch LaunchConfig) (*ProcessHandle, error) {
	if launch.Address == "" {
		return nil, errors.New("spawner: parent address required")
	}

	binary := s.cfg.BinaryPath
	if binary == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("spawner: resolve binary: %w", err)
		}
		binary = exe
	}

	args := append([]string(nil), s.cfg.Args...)
	args = append(args, "--mode=child")
	args = append(args, launch.ExtraArgs...)

	//nolint:gosec // G204: This is intentional - we're launching child processes as part of the spawner functionality.
	// The binary path comes from configuration and is validated by the caller.
	// This is a core feature of the spawner package.
	cmd := exec.CommandContext(ctx, binary, args...)
	if s.cfg.WorkingDir != "" {
		cmd.Dir = s.cfg.WorkingDir
	}

	env := append(os.Environ(), s.cfg.Env...)
	env = append(env,
		fmt.Sprintf("%s=%s", child.EnvMode, child.ModeChild),
		fmt.Sprintf("%s=%s", child.EnvParentAddress, launch.Address),
	)
	if launch.AuthToken != "" {
		env = append(env, fmt.Sprintf("%s=%s", child.EnvParentToken, launch.AuthToken))
	}
	if launch.ChildType != "" {
		env = append(env, fmt.Sprintf("%s=%s", child.EnvChildType, launch.ChildType))
	}
	if len(launch.Metadata) > 0 {
		metaJSON, err := json.Marshal(launch.Metadata)
		if err != nil {
			return nil, fmt.Errorf("spawner: marshal metadata: %w", err)
		}
		env = append(env, fmt.Sprintf("%s=%s", child.EnvChildMetadata, string(metaJSON)))
	}
	if launch.TLS.CertPath != "" {
		env = append(env, fmt.Sprintf("%s=%s", child.EnvTLSCert, launch.TLS.CertPath))
	}
	if launch.TLS.KeyPath != "" {
		env = append(env, fmt.Sprintf("%s=%s", child.EnvTLSKey, launch.TLS.KeyPath))
	}
	if launch.TLS.CAPath != "" {
		env = append(env, fmt.Sprintf("%s=%s", child.EnvTLSCA, launch.TLS.CAPath))
	}
	if len(launch.Pools) > 0 {
		poolsJSON, err := json.Marshal(launch.Pools)
		if err != nil {
			return nil, fmt.Errorf("spawner: marshal pools: %w", err)
		}
		env = append(env, fmt.Sprintf("%s=%s", child.EnvChildPools, string(poolsJSON)))
	}
	env = append(env, launch.ExtraEnv...)
	cmd.Env = env

	var stdout, stderr io.ReadCloser
	if !s.cfg.DisableOutputCapture {
		stdoutPipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("spawner: stdout pipe: %w", err)
		}
		stderrPipe, err := cmd.StderrPipe()
		if err != nil {
			return nil, fmt.Errorf("spawner: stderr pipe: %w", err)
		}
		stdout = stdoutPipe
		stderr = stderrPipe
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("spawner: start process: %w", err)
	}

	handle := &ProcessHandle{
		cmd:           cmd,
		reporter:      s.cfg.Reporter,
		stdout:        stdout,
		stderr:        stderr,
		done:          make(chan error, 1),
		shutdownGrace: s.cfg.ShutdownGrace,
		executor:      s.executor,
		clock:         s.clock,
	}
	if !s.cfg.DisableOutputCapture {
		s.executor.Go(fmt.Sprintf("capture-stdout-%d", cmd.Process.Pid), func() {
			handle.capture("stdout", stdout)
		})
		s.executor.Go(fmt.Sprintf("capture-stderr-%d", cmd.Process.Pid), func() {
			handle.capture("stderr", stderr)
		})
	}
	s.executor.Go(fmt.Sprintf("wait-process-%d", cmd.Process.Pid), func() {
		handle.wait()
	})

	s.cfg.Reporter.Report(diagnostics.Event{
		Component: "spawner.process",
		Severity:  diagnostics.SeverityInfo,
		Metadata: map[string]any{
			"pid":    cmd.Process.Pid,
			"binary": binary,
			"args":   strings.Join(args, " "),
		},
	})

	return handle, nil
}

// ProcessHandle represents a running child process.
// It implements the ProcessHandle interface defined in interfaces.go.
type ProcessHandle struct {
	cmd           *exec.Cmd
	reporter      Reporter
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	done          chan error
	shutdownGrace time.Duration
	executor      *concurrency.DeterministicExecutor
	clock         clock.Clock
}

// PID returns the process identifier.
func (h *ProcessHandle) PID() int {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

// Wait blocks until the process exits.
func (h *ProcessHandle) Wait(ctx context.Context) error {
	if h == nil {
		return nil
	}
	// Use PrioritySelect for deterministic channel selection
	c := h.clock
	if c == nil {
		c = clock.DefaultClock()
	}
	seq := c.Sequence()
	cases := []concurrency.SelectCase{
		{
			Chan:     ctx.Done(),
			Priority: 10, // Higher priority: context cancellation
			Sequence: seq,
		},
		{
			Chan:     h.done,
			Priority: 1, // Lower priority: done channel
			Sequence: seq + 1,
		},
	}
	chosen, recv, recvOK := concurrency.PrioritySelect(cases, c)
	if chosen == 0 {
		// Context cancelled
		return ctx.Err()
	}
	if !recvOK {
		// Channel closed without error
		return nil
	}
	if recv == nil {
		// No error received
		return nil
	}
	err, ok := recv.(error)
	if !ok {
		// Unexpected type
		return nil
	}
	return err
}

// Stop terminates the process.
func (h *ProcessHandle) Stop() error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	c := h.clock
	if c == nil {
		c = clock.DefaultClock()
	}
	if h.shutdownGrace > 0 {
		_ = h.cmd.Process.Signal(os.Interrupt)
		// Use PrioritySelect for deterministic channel selection
		seq := c.Sequence()
		cases := []concurrency.SelectCase{
			{
				Chan:     h.done,
				Priority: 10, // Higher priority: process done
				Sequence: seq,
			},
			{
				Chan:     c.After(h.shutdownGrace),
				Priority: 1, // Lower priority: timeout
				Sequence: seq + 1,
			},
		}
		chosen, recv, recvOK := concurrency.PrioritySelect(cases, c)
		if chosen == 0 && recvOK {
			// Process done
			err := recv.(error)
			return err
		}
		// Timeout expired, continue to kill
	}
	if err := h.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	// Non-blocking check for final error
	select {
	case err := <-h.done:
		return err
	default:
		return nil
	}
}

func (h *ProcessHandle) wait() {
	err := h.cmd.Wait()
	h.done <- err
	close(h.done)
	if h.reporter != nil {
		severity := diagnostics.SeverityInfo
		if err != nil {
			severity = diagnostics.SeverityError
		}
		h.reporter.Report(diagnostics.Event{
			Component: "spawner.process",
			Severity:  severity,
			Err:       err,
			Metadata: map[string]any{
				"pid": h.PID(),
			},
		})
	}
}

func (h *ProcessHandle) capture(stream string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if h.reporter != nil {
			h.reporter.Report(diagnostics.Event{
				Component: fmt.Sprintf("spawner.process.%s", stream),
				Severity:  diagnostics.SeverityInfo,
				Metadata: map[string]any{
					"pid":  h.PID(),
					"line": scanner.Text(),
				},
			})
		}
	}
}
