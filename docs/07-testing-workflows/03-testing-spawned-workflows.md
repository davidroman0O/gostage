# 03 - Testing Spawned Workflows

Testing a workflow that runs in a separate process presents a unique challenge: your test needs to act as the parent process, spawn the child, and then verify the results communicated back from the child.

A powerful and self-contained way to write these kinds of integration tests in Go is to use the `TestMain` function to create a single test binary that can act as both the parent *and* the child.

## The `TestMain` Pattern for Spawning

The `go test` command compiles your code and test files into a single executable. We can leverage this to create a "dual-mode" binary.

The pattern relies on an environment variable (e.g., `GOSTAGE_EXEC_CHILD`) to signal which mode the binary should run in.

1.  **The Parent (Test Function)**: Your `Test...` function will set up the parent `Runner`, define the `SubWorkflowDef`, and then execute the test binary *again*, but this time with the environment variable set.
2.  **The Child (`TestMain`)**: When the new process starts, the `TestMain` function is the first thing to run. It checks for the environment variable. If it's present, `TestMain` bypasses the standard test execution and instead calls a `childMain` function, which contains the logic for the child worker.

Here's how you structure the `TestMain` function in your test file (e.g., `spawn_test.go`):

```go
func TestMain(m *testing.M) {
	// 1. Check for the environment variable.
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		// 2. If it's set, we are the child. Run the child logic and exit.
		childMain()
		return
	}

	// 3. If it's not set, we are the parent. Run the tests normally.
	os.Exit(m.Run())
}
```

### Parent Test Logic

Your actual test function acts as the parent. It sets up handlers, defines the sub-workflow, and then calls a helper function to execute the test binary as a child process.

```go
func TestSpawn(t *testing.T) {
    // Parent logic here...
    parentRunner := gostage.NewRunner()
    // ... register handlers ...

    subWorkflowDef := gostage.SubWorkflowDef{ /* ... */ }

    // This helper function will execute the test binary again with the env var.
    err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
    assert.NoError(t, err)

    // ... assert results received from the child ...
}
```

### Child Worker Logic

The `childMain` function contains all the logic the child needs: it registers actions, reads the definition from `stdin`, reconstructs the workflow, and executes it.

```go
func childMain() {
    registerActions() // Critical: child must know how to build actions.

	workflowDef, _ := gostage.ReadWorkflowDefinitionFromStdin()
	childRunner, _ := gostage.NewChildRunner(*workflowDef)
	wf, _ := gostage.NewWorkflowFromDef(workflowDef)

    // The logger should be configured to send messages back via the broker.
	childRunner.Execute(context.Background(), wf, brokerLogger)
}
```

This pattern creates a fully self-contained integration test for your spawned workflows without needing separate binary builds or complex test scripts. 