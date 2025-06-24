package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// SimpleConsoleLogger is a basic logger that prints to the console.
type SimpleConsoleLogger struct{}

func (l *SimpleConsoleLogger) Info(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Debug(format string, args ...interface{}) {}
func (l *SimpleConsoleLogger) Warn(format string, args ...interface{})  {}
func (l *SimpleConsoleLogger) Error(format string, args ...interface{}) {}

// --- Actions that simulate work ---
type ShortTaskAction struct {
	gostage.BaseAction
}

func (a *ShortTaskAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> Running short task...")
	time.Sleep(50 * time.Millisecond)
	return nil
}

type LongTaskAction struct {
	gostage.BaseAction
}

func (a *LongTaskAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> Running long task...")
	time.Sleep(150 * time.Millisecond)
	return nil
}

// TimingMiddleware is a StageMiddleware that wraps the execution of all actions
// within a stage to measure their total execution time.
func TimingMiddleware(stageName string) gostage.StageMiddleware {
	return func(next gostage.StageRunnerFunc) gostage.StageRunnerFunc {
		return func(ctx context.Context, stage *gostage.Stage, wf *gostage.Workflow, logger gostage.Logger) error {
			// This code runs BEFORE any actions in the stage.
			startTime := time.Now()
			logger.Info("Starting timed execution for stage: '%s'", stage.Name)

			// This executes all the actions within the stage.
			err := next(ctx, stage, wf, logger)

			// This code runs AFTER all actions in the stage have completed.
			duration := time.Since(startTime)
			logger.Info("Finished timed execution for stage '%s'. Duration: %v", stage.Name, duration)

			// Store the result in the workflow's main store.
			storeKey := fmt.Sprintf("timing.%s", stageName)
			wf.Store.Put(storeKey, duration)

			return err
		}
	}
}

func main() {
	// Create a workflow.
	workflow := gostage.NewWorkflow(
		"timing-demo",
		"Timing Middleware Demo",
		"Demonstrates timing actions with stage middleware.",
	)

	// --- Stage 1: Short Tasks ---
	shortTasksStage := gostage.NewStage("short-tasks", "Short Tasks", "A stage with quick actions.")
	// Apply the timing middleware to this specific stage.
	shortTasksStage.Use(TimingMiddleware("short-tasks"))
	shortTasksStage.AddAction(&ShortTaskAction{BaseAction: gostage.NewBaseAction("short-task-1", "")})
	shortTasksStage.AddAction(&ShortTaskAction{BaseAction: gostage.NewBaseAction("short-task-2", "")})

	// --- Stage 2: Long Tasks ---
	longTasksStage := gostage.NewStage("long-tasks", "Long Tasks", "A stage with slower actions.")
	// Apply the same timing middleware to this stage as well.
	longTasksStage.Use(TimingMiddleware("long-tasks"))
	longTasksStage.AddAction(&LongTaskAction{BaseAction: gostage.NewBaseAction("long-task-1", "")})
	longTasksStage.AddAction(&LongTaskAction{BaseAction: gostage.NewBaseAction("long-task-2", "")})

	// Add stages to the workflow
	workflow.AddStage(shortTasksStage)
	workflow.AddStage(longTasksStage)

	// Execute the workflow
	runner := gostage.NewRunner()
	logger := &SimpleConsoleLogger{}
	fmt.Println("Executing workflow with timing middleware...")
	if err := runner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}
	fmt.Println("\nWorkflow completed.")

	// Print the results from the store
	fmt.Println("\n--- Timing Results from Store ---")
	shortTaskTime, _ := store.Get[time.Duration](workflow.Store, "timing.short-tasks")
	longTaskTime, _ := store.Get[time.Duration](workflow.Store, "timing.long-tasks")

	fmt.Printf("  - Stage 'short-tasks' took: %v\n", shortTaskTime)
	fmt.Printf("  - Stage 'long-tasks' took:  %v\n", longTaskTime)
}
