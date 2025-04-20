package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/davidroman0O/gostate"
	"github.com/davidroman0O/gostate/examples/common"
	"github.com/davidroman0O/gostate/store"
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
	gostate.BaseAction
	operation FileOperation
}

// NewFileAction creates a new file action with the specified operation
func NewFileAction(operation FileOperation) *FileAction {
	name := fmt.Sprintf("file-%s", operation)
	description := fmt.Sprintf("Performs %s operation on files", operation)

	return &FileAction{
		BaseAction: gostate.NewBaseAction(name, description),
		operation:  operation,
	}
}

// Execute handles the file operation
func (a *FileAction) Execute(ctx *gostate.ActionContext) error {
	// Get source and destination from store
	source, err := store.Get[string](ctx.Store, "file.source")
	if err != nil {
		return fmt.Errorf("missing file.source in store: %w", err)
	}

	// For operations like delete and check_exist, we don't need a destination
	needsDest := a.operation == Copy || a.operation == Move

	var dest string
	if needsDest {
		dest, err = store.Get[string](ctx.Store, "file.destination")
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
		if err := ctx.Store.Put("file.exists", exists); err != nil {
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
func CreateFileWorkflowExample() *gostate.Workflow {
	// Create a workflow for file operations
	wf := gostate.NewWorkflow(
		"file-workflow",
		"File Operations Workflow",
		"Demonstrates how to use file operations in a workflow",
	)

	// Create a stage for file operations
	stage := gostate.NewStage(
		"file-operations",
		"File Operations",
		"Performs various file operations",
	)

	// Add initial data to the stage
	stage.InitialStore.Put("file.source", "/tmp/source.txt")
	stage.InitialStore.Put("file.destination", "/tmp/destination.txt")

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

	if err := wf.Execute(ctx, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}
