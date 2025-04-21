package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/examples/common"
	"github.com/davidroman0O/gostage/store"
)

// FileOperation represents the different operations that can be performed on files
type FileOperation string

const (
	Copy       FileOperation = "copy"
	Move       FileOperation = "move"
	Delete     FileOperation = "delete"
	MakeDir    FileOperation = "mkdir"
	CheckExist FileOperation = "check_exist"
)

// FileAction demonstrates a platform-aware action that handles file operations
type FileAction struct {
	gostage.BaseAction
	operation FileOperation
}

// NewFileAction creates a new file action with the specified operation
func NewFileAction(operation FileOperation) *FileAction {
	name := fmt.Sprintf("file-%s", operation)
	description := fmt.Sprintf("Performs %s operation on files", operation)

	return &FileAction{
		BaseAction: gostage.NewBaseAction(name, description),
		operation:  operation,
	}
}

// Execute handles the file operation
func (a *FileAction) Execute(ctx *gostage.ActionContext) error {
	// Get source and destination from store
	source, err := store.Get[string](ctx.Store(), "file.source")
	if err != nil {
		return fmt.Errorf("missing file.source in store: %w", err)
	}

	// For operations like delete and check_exist, we don't need a destination
	needsDest := a.operation == Copy || a.operation == Move

	var dest string
	if needsDest {
		dest, err = store.Get[string](ctx.Store(), "file.destination")
		if err != nil {
			return fmt.Errorf("missing file.destination in store: %w", err)
		}
	}

	ctx.Logger.Info("Executing file operation %s on %s", a.operation, source)

	switch a.operation {
	case Copy:
		if err := copyFile(source, dest); err != nil {
			return fmt.Errorf("failed to copy file: %w", err)
		}

	case Move:
		if err := moveFile(source, dest); err != nil {
			return fmt.Errorf("failed to move file: %w", err)
		}

	case Delete:
		if err := deleteFile(source); err != nil {
			return fmt.Errorf("failed to delete file: %w", err)
		}

	case MakeDir:
		if err := os.MkdirAll(source, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

	case CheckExist:
		exists, err := checkFileExists(source)
		if err != nil {
			return fmt.Errorf("failed to check if file exists: %w", err)
		}

		// Store the result in the KV store
		if err := ctx.Store().Put("file.exists", exists); err != nil {
			return fmt.Errorf("failed to store file.exists result: %w", err)
		}

	default:
		return fmt.Errorf("unsupported file operation: %s", a.operation)
	}

	return nil
}

// Helper functions for file operations

func copyFile(src, dst string) error {
	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func moveFile(src, dst string) error {
	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	return os.Rename(src, dst)
}

func deleteFile(path string) error {
	return os.Remove(path)
}

func checkFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Example of how to use the FileAction in a workflow
func CreateFileWorkflowExample() *gostage.Workflow {
	// Create a workflow for file operations
	wf := gostage.NewWorkflow(
		"file-workflow",
		"File Operations Workflow",
		"Demonstrates how to use file operations in a workflow",
	)

	// Create a stage for file operations
	stage := gostage.NewStage(
		"file-operations",
		"File Operations",
		"Performs various file operations",
	)

	// Add initial data to the stage
	stage.SetInitialData("file.source", "/tmp/source.txt")
	stage.SetInitialData("file.destination", "/tmp/destination.txt")

	// Add actions to the stage
	stage.AddAction(NewFileAction(CheckExist))
	stage.AddAction(NewFileAction(Copy))
	stage.AddAction(NewFileAction(Move))

	// Add the stage to the workflow
	wf.AddStage(stage)

	return wf
}

// Main function to run the example
func main() {
	fmt.Println("--- File Operations Workflow Example ---")

	// Create the workflow
	wf := CreateFileWorkflowExample()

	// Print workflow information
	fmt.Printf("Workflow: %s - %s\n", wf.ID, wf.Name)
	fmt.Printf("Description: %s\n", wf.Description)
	fmt.Printf("Stages: %d\n\n", len(wf.Stages))

	// Optional: Create a test file for the workflow to use
	testFile := "/tmp/source.txt"
	err := os.WriteFile(testFile, []byte("This is a test file for the workflow example"), 0644)
	if err != nil {
		fmt.Printf("Error creating test file: %v\n", err)
		return
	}
	fmt.Printf("Created test file: %s\n\n", testFile)

	// Execute the workflow
	fmt.Println("Executing workflow...")

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	// Create a runner
	runner := gostage.NewRunner()

	if err := runner.Execute(ctx, wf, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}

// FileExistsAction represents an action to check if a file exists
type FileExistsAction struct {
	gostage.BaseAction
	PathKey     string
	DefaultPath string
}

// Execute performs the file check
func (a *FileExistsAction) Execute(ctx *gostage.ActionContext) error {
	// First check if we have the file path in the context
	filePath, err := store.Get[string](ctx.Store(), a.PathKey)
	if err != nil {
		// If not found, use the default path
		filePath = a.DefaultPath
	}

	// Check if the file exists
	ctx.Logger.Info("Checking if file exists: %s", filePath)
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist
			ctx.Logger.Info("File does not exist: %s", filePath)

			// Store the result
			if err := ctx.Store().Put("file.exists", false); err != nil {
				return fmt.Errorf("failed to store file existence status: %w", err)
			}

			return nil
		}
		// Some other error occurred
		return fmt.Errorf("error checking file: %w", err)
	}

	// File exists
	ctx.Logger.Info("File exists: %s (size: %d bytes, modified: %s)",
		filePath, info.Size(), info.ModTime().Format(time.RFC3339))

	// Store the result
	if err := ctx.Store().Put("file.exists", true); err != nil {
		return fmt.Errorf("failed to store file existence status: %w", err)
	}

	return nil
}

// SetupFileCheck creates a stage for file checking
func SetupFileCheck(filePath string) *gostage.Stage {
	// Create a stage for file checking
	stage := gostage.NewStage("file-check", "File Check", "Verifies that required files exist")

	// Set initial data for the stage
	stage.SetInitialData("file.path", filePath)
	stage.SetInitialData("file.exists", false) // Default value

	// Add the file existence check action
	stage.AddAction(&FileExistsAction{
		BaseAction: gostage.NewBaseAction("check-file", "Check File Existence"),
		PathKey:    "file.path",
	})

	return stage
}
