# GoStage Spawn Middleware Example

This example demonstrates GoStage's middleware system for enhancing inter-process communication and spawn lifecycle management.

## Overview

This example showcases two types of middleware:

1. **IPC Middleware** - Transforms messages between parent and child processes
2. **Spawn Middleware** - Provides hooks for process lifecycle management

## Middleware Types Demonstrated

### IPC Middleware

- **MessageTransformMiddleware**: Adds timestamps and prefixes to log messages
- **MessageEncryptionMiddleware**: Simulates encryption/decryption for sensitive store data
- **MetricsMiddleware**: Collects communication statistics

### Spawn Middleware

- **ProcessLifecycleMiddleware**: Tracks spawn timing and completion status
- **MetricsMiddleware**: Counts messages and tracks process lifecycle

## Features

- Real child process spawning with middleware enhancement
- Message transformation and filtering
- Encryption simulation for sensitive data
- Process lifecycle tracking with timing
- Communication metrics collection
- Enhanced logging with timestamps and prefixes

## Running the Example

```bash
cd examples/spawn_middleware
go run main.go
```

## Architecture

```
Parent Process                     Child Process
┌─────────────────┐               ┌─────────────────┐
│                 │    stdin      │                 │
│   Runner with   │─────────────→ │   Child Runner  │
│   Middleware    │               │   with Actions  │
│                 │    stdout     │                 │
│                 │←───────────── │                 │
└─────────────────┘               └─────────────────┘
        │                                   │
        ▼                                   ▼
   Middleware Chain                  Message Generation
   ┌─────────────┐                  ┌─────────────────┐
   │ Lifecycle   │                  │ Log Messages    │
   │ Metrics     │                  │ Store Updates   │
   │ Transform   │                  │ Process Info    │
   │ Encryption  │                  │                 │
   └─────────────┘                  └─────────────────┘
```

## Middleware Flow

### IPC Middleware (Outbound):
1. Child creates message
2. MessageTransformMiddleware adds timestamp/prefix
3. MessageEncryptionMiddleware encrypts sensitive data
4. MetricsMiddleware counts message
5. Message sent to parent

### IPC Middleware (Inbound):
1. Parent receives message
2. MetricsMiddleware counts message
3. MessageEncryptionMiddleware decrypts data
4. Message processed by handler

### Spawn Middleware:
1. BeforeSpawn: Process about to start
2. OnChildMessage: Every message received
3. AfterSpawn: Process completed (success or failure)

## Example Output

```
🎭 === MIDDLEWARE DEMONSTRATION ===

📊 [METRICS] Starting metrics collection
🚀 [MIDDLEWARE] About to spawn child process for workflow: middleware-demo
🚀 Starting middleware-enhanced spawn...
📨 [MIDDLEWARE] Received message type: log
[15:04:05.123] [CHILD-INFO] [ENHANCED] Regular log message from PID 12345
📨 [MIDDLEWARE] Received message type: store_put
📦 [STORE] regular_data = public information
📨 [MIDDLEWARE] Received message type: store_put
📦 [STORE] sensitive_data = secret information
⏱️ [MIDDLEWARE] Child process completed in 45.2ms
✅ [MIDDLEWARE] Child process completed successfully

📊 [METRICS] Communication Statistics:
  log: 4 messages
  store_put: 3 messages
  Total bytes: 1247

🎉 Middleware demo completed successfully!

📦 === FINAL STORE STATE ===
  regular_data: public information
  sensitive_data: secret information
  process_id: 12345
```

## Key Learning Points

1. **Middleware Composition**: Multiple middleware can be chained together
2. **Bidirectional Processing**: Middleware works on both outbound and inbound messages
3. **Process Lifecycle**: Hooks available for spawn start, completion, and message flow
4. **Real Child Processes**: Demonstrates actual inter-process communication
5. **Data Transformation**: Shows how to modify messages in transit
6. **Metrics Collection**: Example of cross-cutting concerns implementation 