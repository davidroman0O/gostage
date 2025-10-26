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
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
)

// Reporter allows the spawner to emit diagnostic events.
type Reporter interface {
	Report(diagnostics.Event)
}

// LoggerReporter adapts a function to Reporter.
type LoggerReporter func(diagnostics.Event)

func (fn LoggerReporter) Report(evt diagnostics.Event) {
	if fn != nil {
		fn(evt)
	}
}

// Config controls process spawner behaviour.
type Config struct {
	BinaryPath     string
	Args           []string
	Env            []string
	WorkingDir     string
	MaxRestarts    int
	RestartBackoff time.Duration
	Reporter       Reporter
	ShutdownGrace  time.Duration
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
type ProcessSpawner struct {
	cfg Config
	mu  sync.Mutex
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
		cfg.Reporter = LoggerReporter(func(d diagnostics.Event) {})
	}
	return &ProcessSpawner{cfg: cfg}
}

// SetReporter updates the diagnostics reporter used for child processes.
func (s *ProcessSpawner) SetReporter(rep Reporter) {
	if rep == nil {
		rep = LoggerReporter(func(d diagnostics.Event) {})
	}
	s.mu.Lock()
	s.cfg.Reporter = rep
	s.mu.Unlock()
}

// Reporter returns the currently configured diagnostics reporter.
func (s *ProcessSpawner) Reporter() Reporter {
	s.mu.Lock()
	defer s.mu.Unlock()
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

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("spawner: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("spawner: stderr pipe: %w", err)
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
	}
	go handle.capture("stdout", stdout)
	go handle.capture("stderr", stderr)
	go handle.wait()

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
type ProcessHandle struct {
	cmd           *exec.Cmd
	reporter      Reporter
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	done          chan error
	shutdownGrace time.Duration
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-h.done:
		return err
	}
}

// Stop terminates the process.
func (h *ProcessHandle) Stop() error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return nil
	}
	if h.shutdownGrace > 0 {
		_ = h.cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-h.done:
			return err
		case <-time.After(h.shutdownGrace):
		}
	}
	if err := h.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
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
