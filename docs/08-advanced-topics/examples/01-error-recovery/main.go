package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/davidroman0O/gostage"
)

// SimpleConsoleLogger is a basic logger that prints to the console.
type SimpleConsoleLogger struct{}

func (l *SimpleConsoleLogger) Info(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Warn(format string, args ...interface{}) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Error(format string, args ...interface{}) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Debug(format string, args ...interface{}) {}

// --- Actions ---

// SuccessAction is an action that always succeeds.
type SuccessAction struct {
	gostage.BaseAction
}

func (a *SuccessAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> SuccessAction executed.")
	return nil
}

// FailingAction is an action that returns a specific, "non-critical" error.
type FailingAction struct {
	gostage.BaseAction
}

func (a *FailingAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> FailingAction executed.")
	return errors.New("a non-critical error occurred, but it's okay")
}

// CriticalFailingAction returns an error that should not be recovered from.
type CriticalFailingAction struct {
	gostage.BaseAction
}

func (a *CriticalFailingAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> CriticalFailingAction executed.")
	return errors.New("CRITICAL: database connection lost")
}

// --- Middleware ---

// ErrorRecoveryMiddleware is a runner middleware that inspects errors and
// recovers from them if they match a specific pattern.
func ErrorRecoveryMiddleware(recoverablePatterns []string) gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			err := next(ctx, wf, logger)

			if err != nil {
				for _, pattern := range recoverablePatterns {
					if strings.Contains(err.Error(), pattern) {
						logger.Warn("Recoverable error in workflow '%s': %v. Marking as success.", wf.ID, err)
						// By returning nil, we "swallow" the error.
						return nil
					}
				}
				logger.Error("Unrecoverable error in workflow '%s': %v", wf.ID, err)
			}
			return err
		}
	}
}

func main() {
	logger := &SimpleConsoleLogger{}

	// Create a runner with our error handling middleware.
	// It will recover from any error containing "non-critical".
	runner := gostage.NewRunner(
		gostage.WithMiddleware(ErrorRecoveryMiddleware([]string{"non-critical"})),
	)

	// --- Workflow 1: Should "succeed" due to error recovery ---
	fmt.Println("--- Running Workflow 1 (with recoverable error) ---")
	wf1 := gostage.NewWorkflow("wf-recover", "Recoverable Workflow", "")
	stage1 := gostage.NewStage("stage1", "", "")
	stage1.AddAction(&FailingAction{BaseAction: gostage.NewBaseAction("failing-action", "")})
	wf1.AddStage(stage1)

	err1 := runner.Execute(context.Background(), wf1, logger)
	if err1 != nil {
		fmt.Printf("  ❌ FAILED: Workflow 1 returned an unexpected error: %v\n", err1)
	} else {
		fmt.Printf("  ✅ SUCCESS: Workflow 1 completed without a propagating error.\n")
	}

	// --- Workflow 2: Should fail and propagate the error ---
	fmt.Println("\n--- Running Workflow 2 (with critical error) ---")
	wf2 := gostage.NewWorkflow("wf-critical", "Critical Workflow", "")
	stage2 := gostage.NewStage("stage2", "", "")
	stage2.AddAction(&CriticalFailingAction{BaseAction: gostage.NewBaseAction("critical-action", "")})
	wf2.AddStage(stage2)

	err2 := runner.Execute(context.Background(), wf2, logger)
	if err2 != nil {
		fmt.Printf("  ✅ SUCCESS: Workflow 2 correctly failed with a critical error: %v\n", err2)
	} else {
		fmt.Printf("  ❌ FAILED: Workflow 2 should have returned an error but didn't.\n")
	}
}
