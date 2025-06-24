package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage"
)

// SimpleConsoleLogger is a basic logger that prints to the console.
type SimpleConsoleLogger struct{}

func (l *SimpleConsoleLogger) Debug(format string, args ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Info(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Warn(format string, args ...interface{}) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}
func (l *SimpleConsoleLogger) Error(format string, args ...interface{}) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}

// StructuredLoggingMiddleware is a runner middleware that adds structured
// logging before and after every workflow execution.
func StructuredLoggingMiddleware() gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			// Use a structured format for logging.
			logger.Info(
				"WorkflowStart: ID=%s Name='%s' Tags=%v",
				wf.ID,
				wf.Name,
				wf.Tags,
			)

			// Execute the next function in the chain (the workflow itself).
			err := next(ctx, wf, logger)

			// Log the result.
			if err != nil {
				logger.Error(
					"WorkflowEnd: ID=%s Name='%s' Status=FAILED Error='%v'",
					wf.ID,
					wf.Name,
					err,
				)
			} else {
				logger.Info(
					"WorkflowEnd: ID=%s Name='%s' Status=SUCCESS",
					wf.ID,
					wf.Name,
				)
			}

			return err
		}
	}
}

// A simple action for demonstration.
type MyAction struct {
	gostage.BaseAction
}

func (a *MyAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> MyAction is executing within the workflow...")
	time.Sleep(100 * time.Millisecond) // Simulate work
	return nil
}

func main() {
	// 1. Create a runner and apply the custom logging middleware.
	runner := gostage.NewRunner(
		gostage.WithMiddleware(StructuredLoggingMiddleware()),
	)

	// 2. Create a workflow.
	workflow := gostage.NewWorkflowWithTags(
		"logged-workflow",
		"Logged Workflow",
		"A workflow to demonstrate custom logging.",
		[]string{"demo", "logging"},
	)

	// 3. Create a stage and add an action.
	stage := gostage.NewStage("main-stage", "Main Stage", "")
	stage.AddAction(&MyAction{
		BaseAction: gostage.NewBaseAction("my-action", "A simple demo action"),
	})
	workflow.AddStage(stage)

	// 4. Execute the workflow.
	// The runner will automatically wrap the execution with our middleware.
	fmt.Println("Executing workflow with custom logging middleware...")
	logger := &SimpleConsoleLogger{}
	if err := runner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}

	fmt.Println("\nWorkflow completed.")
}
