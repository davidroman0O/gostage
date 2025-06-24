# GoStage Spawn Process Examples

This directory contains examples demonstrating GoStage's child process spawning capabilities.

## Available Examples

### 1. Basic Spawn Example (this directory)
- **Location**: `examples/spawn_process/`
- **Purpose**: Demonstrates basic child process spawning with IPC communication
- **Features**: Real child processes, different PIDs, file operations, store synchronization

### 2. Middleware Spawn Example
- **Location**: `examples/spawn_middleware/`
- **Purpose**: Demonstrates advanced middleware system for IPC and process lifecycle
- **Features**: Message transformation, encryption simulation, metrics collection, lifecycle hooks

## Quick Start

### Run Basic Example
```bash
cd examples/spawn_process
go run main.go
```

### Run Middleware Example
```bash
cd examples/spawn_middleware
go run main.go
```

---

## Basic Spawn Example Documentation

This example demonstrates the fundamental child process spawning functionality of GoStage.

### What This Example Proves

1. **Real Child Processes**: Creates actual child processes with different PIDs
2. **Inter-Process Communication**: Parent and child exchange structured messages
3. **Store Synchronization**: Child can send data updates back to parent
4. **File System Operations**: Child creates files that parent can verify
5. **Process Information**: Demonstrates process hierarchy and identification

### Example Architecture

```
Parent Process (main.go)                 Child Process (--gostage-child)
┌─────────────────────────┐              ┌─────────────────────────┐
│                         │   stdin      │                         │
│  gostage.Runner         │─────────────→│  Child gostage.Runner   │
│  - Message Handlers     │              │  - Actions Execution    │
│  - Store Management     │   stdout     │  - Logger via IPC       │
│                         │←─────────────│                         │
└─────────────────────────┘              └─────────────────────────┘
         │                                          │
         ▼                                          ▼
   Collects Results                           Executes Workflow
   ┌─────────────────┐                       ┌─────────────────┐
   │ - Process IDs   │                       │ - ProcessInfo   │
   │ - Store Data    │                       │ - SimpleTest    │
   │ - File Paths    │                       │ - FileOps       │
   │ - Log Messages  │                       │                 │
   └─────────────────┘                       └─────────────────┘
```

### Actions Included

1. **ProcessInfoAction**: Reports comprehensive process information (PID, parent PID, hostname, working directory)
2. **SimpleTestAction**: Simple "hello world" demonstration with PID logging
3. **FileOperationAction**: Creates a file in `/tmp/` with process information

### Key Learning Points

1. **Process Isolation**: Child runs in completely separate process space
2. **IPC Communication**: Structured JSON message passing via stdin/stdout
3. **Error Handling**: Proper error propagation between processes
4. **Store Synchronization**: Parent receives and stores child data updates
5. **File System Verification**: Parent can verify child's file operations

### Example Output

```
🚀 PARENT PROCESS STARTED - PID: 12345
📋 Parent process 12345 spawning child to execute comprehensive workflow
🔥 CHILD PROCESS STARTED - PID: 12346, Parent PID: 12345
✅ Child process 12346 executing workflow: spawn-demo-workflow
[CHILD-INFO] === CHILD PROCESS INFORMATION ===
[CHILD-INFO] Process ID: 12346
[CHILD-INFO] Parent Process ID: 12345
[CHILD-INFO] Hostname: hostname.local
📦 Store update: child_process_id = 12346
📦 Store update: child_parent_id = 12345
📦 Store update: child_hostname = hostname.local
[CHILD-INFO] Hello from child process 12346
📦 Store update: child_pid = 12346
[CHILD-INFO] Child process 12346 creating file: /tmp/child_process_12346.txt
[CHILD-INFO] File created successfully by process 12346
📦 Store update: created_file = /tmp/child_process_12346.txt
⏱️  Child process execution completed in 25ms
✅ Child process execution completed successfully!

🔍 Child Process Verification:
  ✅ Child had different PID: 12346 (Parent: 12345)
  ✅ Child's parent PID matches: 12345
  ✅ Child hostname: hostname.local
  ✅ Child created file: /tmp/child_process_12346.txt
  ✅ File exists and is accessible from parent!
  📄 File content:
This file was created by child process 12346 at 2023-06-10 14:30:45
Parent PID: 12345

🎉 Example completed successfully!
   This proves that gostage.Runner.Spawn() creates real child processes
   with separate PIDs that can communicate back to the parent!
```

## Development Usage

This pattern is useful for:

- **Isolation**: Running untrusted or risky operations in separate processes
- **Scalability**: Distributing work across multiple processes
- **Fault Tolerance**: Child process failures don't crash the parent
- **Resource Management**: Better control over memory and CPU usage
- **Sandboxing**: Limiting access to system resources per process

## See Also

- **Middleware Example**: See `../spawn_middleware/` for advanced middleware capabilities
- **GoStage Documentation**: Main project documentation for full API reference 