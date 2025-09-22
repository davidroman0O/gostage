// Package `process` routing provides child process handler registration
// following the idiomatic Go pattern similar to http.HandleFunc.
package process

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/davidroman0O/gostage/v2/types"
)

// ChildHandler represents a function that handles child process execution
// for a specific process type. Follows Go's standard function signature pattern.
type ChildHandler func(ctx context.Context) error

var (
	// childHandlers maps process types to their handler functions
	childHandlers = make(map[string]ChildHandler)

	// handlerMutex protects concurrent access to childHandlers
	handlerMutex sync.RWMutex
)

// HandleChildProcess registers a handler function for a specific child process type.
// This follows the idiomatic Go pattern like http.HandleFunc for familiar developer experience.
//
// Example usage:
//
//	spawn.HandleChildProcess("test-worker", func(ctx context.Context) error {
//	    fmt.Println("Test worker executing")
//	    return nil
//	})
//
//	spawn.HandleChildProcess("gpu-worker", func(ctx context.Context) error {
//	    return initializeGPUWorker(ctx)
//	})
func HandleChildProcess(processType string, handler ChildHandler) {
	if processType == "" {
		panic("spawn: empty process type")
	}
	if handler == nil {
		panic("spawn: nil handler")
	}

	handlerMutex.Lock()
	defer handlerMutex.Unlock()

	childHandlers[processType] = handler
}

// extractProcessType extracts the process type from command line arguments.
// Returns "default" if no process type is specified.
func extractProcessType() string {
	for i, arg := range os.Args {
		if arg == "--process-type" && i+1 < len(os.Args) {
			return os.Args[i+1]
		}
	}
	return "default"
}

// executeChildHandler executes the registered handler for the given process type.
// Returns true if a handler was found and executed, false otherwise.
func executeChildHandler(processType string, ctx context.Context) (bool, error) {
	handlerMutex.RLock()
	handler, exists := childHandlers[processType]
	handlerMutex.RUnlock()

	if !exists {
		return false, nil
	}

	return true, handler(ctx)
}

// GetRegisteredProcessTypes returns all currently registered process types.
// Useful for debugging and introspection.
func GetRegisteredProcessTypes() []string {
	handlerMutex.RLock()
	defer handlerMutex.RUnlock()

	types := make([]string, 0, len(childHandlers))
	for processType := range childHandlers {
		types = append(types, processType)
	}

	return types
}

// IsProcessTypeRegistered checks if a handler is registered for the given process type.
func IsProcessTypeRegistered(processType string) bool {
	handlerMutex.RLock()
	defer handlerMutex.RUnlock()

	_, exists := childHandlers[processType]
	return exists
}

// ClearChildHandlers removes all registered child handlers.
// Primarily useful for testing to ensure clean state.
func ClearChildHandlers() {
	handlerMutex.Lock()
	defer handlerMutex.Unlock()

	childHandlers = make(map[string]ChildHandler)
}

// defaultChildMain provides fallback behavior when no specific handler is registered.
// This maintains backward compatibility with existing child process patterns.
func defaultChildMain() {
	fmt.Println("Default child process started")

	// Extract basic information
	processType := extractProcessType()
	fmt.Printf("Process type: %s\n", processType)

	// Log arguments for debugging
	for i, arg := range os.Args[1:] {
		fmt.Printf("Arg[%d]: %s\n", i, arg)
	}

	// Extract worker info if available
	if workerInfo := extractWorkerInfoFromArgs(); workerInfo != nil {
		fmt.Printf("Worker info: ID=%s, Type=%s, Tags=%v\n",
			workerInfo.ID, workerInfo.Type, workerInfo.Tags)
	}

	// Extract connection details
	if host, port, err := parseConnectionDetails(); err == nil {
		fmt.Printf("Parent connection: %s:%d\n", host, port)
	}

	fmt.Println("Default child process completed")
}

// extractWorkerInfoFromArgs extracts worker information from command line arguments.
func extractWorkerInfoFromArgs() *types.ProcessInfo {
	info := &types.ProcessInfo{
		Tags: []string{},
		Meta: make(map[string]interface{}),
	}

	hasWorkerData := false

	// TOO: this need to be rewritten
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--worker-id=") {
			info.ID = strings.TrimPrefix(arg, "--worker-id=")
			hasWorkerData = true
		} else if strings.HasPrefix(arg, "--worker-type=") {
			info.Type = strings.TrimPrefix(arg, "--worker-type=")
			hasWorkerData = true
		} else if strings.HasPrefix(arg, "--worker-tags=") {
			tagsStr := strings.TrimPrefix(arg, "--worker-tags=")
			if tagsStr != "" {
				info.Tags = strings.Split(tagsStr, ",")
				hasWorkerData = true
			}
		} else if strings.HasPrefix(arg, "--worker-meta=") {
			metaStr := strings.TrimPrefix(arg, "--worker-meta=")
			if strings.Contains(metaStr, "=") {
				parts := strings.SplitN(metaStr, "=", 2)
				if len(parts) == 2 {
					info.Meta[parts[0]] = parts[1]
					hasWorkerData = true
				}
			}
		}
	}

	// Return nil if no worker data found
	if !hasWorkerData {
		return nil
	}

	return info
}

// parseConnectionDetails extracts parent connection details from environment/args.
func parseConnectionDetails() (string, int, error) {
	// Check environment variables first
	// TODO: we don't do that anymore, remove the env
	if host := os.Getenv("GOSTAGE_PARENT_HOST"); host != "" {
		if portStr := os.Getenv("GOSTAGE_PARENT_PORT"); portStr != "" {
			if port, err := strconv.Atoi(portStr); err == nil {
				return host, port, nil
			}
		}
	}

	// Parse command line arguments as fallback
	var host string = "localhost"
	var port int = 50051

	// TODO; do we need to re-write that?
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--parent-host=") {
			host = strings.TrimPrefix(arg, "--parent-host=")
		} else if strings.HasPrefix(arg, "--parent-port=") {
			if p, err := strconv.Atoi(strings.TrimPrefix(arg, "--parent-port=")); err == nil {
				port = p
			}
		}
	}

	return host, port, nil
}
