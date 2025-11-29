# GoStage V3 - Phased Implementation Plan

**Status**: Master Implementation Plan
**Version**: 1.0
**Last Updated**: November 2025
**Related**: [BROKER_ARCHITECTURE.md](./BROKER_ARCHITECTURE.md) | [boundaries-framework](./boundaries-framework/)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Phase Overview](#phase-overview)
3. [Phase 1: SQLite Broker + Workflows](#phase-1-sqlite-broker--workflows)
4. [Phase 2: Services Layer](#phase-2-services-layer)
5. [Phase 3: NATS JetStream Distributed](#phase-3-nats-jetstream-distributed)
6. [SDK Design](#sdk-design)
7. [Boundaries Integration](#boundaries-integration)
8. [Migration Plan](#migration-plan)
9. [Implementation Timeline](#implementation-timeline)
10. [Success Metrics](#success-metrics)
11. [Risk Assessment](#risk-assessment)

---

## Executive Summary

GoStage V3 follows a **three-phase implementation plan** that builds functionality incrementally while maintaining a consistent architecture:

| Phase | Focus | Broker | Throughput | Deployment |
|-------|-------|--------|------------|------------|
| **1** | Workflows + Actions | SQLite | 1-5k/sec | Same machine |
| **2** | Services (Moleculer-like) | SQLite | 1-5k/sec | Same machine |
| **3** | Distributed | NATS JetStream | 10-100k/sec | Multi-machine |

**Key Design Principles**:
- **Same Broker interface** across all phases - only implementation changes
- **SDK provides external access** - submit work without running full runtime
- **Boundaries-framework compliant** - all changes follow layer architecture
- **Backward compatible** - each phase builds on previous without breaking changes

---

## Phase Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GOSTAGE V3 PHASES                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Phase 1: WORKFLOWS                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐    │   │
│  │  │ Actions  │───▶│  Workflows   │───▶│  SQLite Broker     │    │   │
│  │  └──────────┘    └──────────────┘    │  (embedded, WAL)   │    │   │
│  │                                       └────────────────────┘    │   │
│  │  Capacity: ~1-5k workflows/sec                                  │   │
│  │  Deployment: Single machine, multi-process                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                  │                                      │
│                                  ▼                                      │
│  Phase 2: SERVICES                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐    │   │
│  │  │ Actions  │───▶│  Workflows   │───▶│                    │    │   │
│  │  └──────────┘    └──────────────┘    │  SQLite Broker     │    │   │
│  │                                       │  + Service Registry│    │   │
│  │  ┌──────────────────────────┐        │                    │    │   │
│  │  │  Services (Moleculer-like)│───────▶│                    │    │   │
│  │  │  broker.Call("users.get") │        └────────────────────┘    │   │
│  │  └──────────────────────────┘                                   │   │
│  │  NEW: Service definition, registration, discovery, calls        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                  │                                      │
│                                  ▼                                      │
│  Phase 3: DISTRIBUTED                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐    │   │
│  │  │ Actions  │───▶│  Workflows   │───▶│                    │    │   │
│  │  └──────────┘    └──────────────┘    │  NATS JetStream    │    │   │
│  │                                       │  Broker            │    │   │
│  │  ┌──────────────────────────┐        │  (distributed)     │    │   │
│  │  │  Services                │────────▶│                    │    │   │
│  │  └──────────────────────────┘        └────────────────────┘    │   │
│  │                                              │                  │   │
│  │  Capacity: ~10-100k workflows/sec            │                  │   │
│  │  Deployment: Multi-machine cluster    ┌──────┴──────┐          │   │
│  │                                       │ NATS Cluster │          │   │
│  │                                       └─────────────┘          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: SQLite Broker + Workflows

### Overview

Phase 1 establishes the foundation: **workflow execution with SQLite as the coordination broker**.

**What Phase 1 Delivers**:
- Complete workflow execution engine
- Multi-process coordination via SQLite WAL
- SDK for external work submission
- Proven broker pattern for future phases

**What Phase 1 Does NOT Include**:
- Service layer (Phase 2)
- Multi-machine distribution (Phase 3)
- NATS/Kafka/RabbitMQ transports (Phase 3)

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PHASE 1 ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    SQLite Broker                         │   │
│  │                  (Coordination Point)                    │   │
│  │                                                         │   │
│  │  Tables:                                                │   │
│  │  ├── broker_nodes (registration, heartbeats)            │   │
│  │  ├── broker_queue (work items, claims)                  │   │
│  │  └── broker_results (completed work)                    │   │
│  │                                                         │   │
│  │  WAL Mode: Multi-reader, serialized-writer              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│           ┌──────────────────┼──────────────────┐              │
│           │                  │                  │              │
│           ▼                  ▼                  ▼              │
│      ┌─────────┐        ┌─────────┐        ┌─────────┐        │
│      │ Worker1 │        │ Worker2 │        │ Worker3 │        │
│      │(Process)│        │(Process)│        │(Process)│        │
│      └─────────┘        └─────────┘        └─────────┘        │
│                                                                 │
│  Work Flow:                                                     │
│  1. External client uses SDK → Submit(work) → INSERT broker_queue
│  2. Workers poll → Claim(types) → UPDATE WHERE state='pending' │
│  3. Worker processes → Complete(result) → UPDATE + INSERT result
│  4. Client polls/waits → GetResult(id) → SELECT from results   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Database Schema

```sql
-- Node registration and health tracking
CREATE TABLE broker_nodes (
    id TEXT PRIMARY KEY,
    started_at INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    capabilities TEXT,  -- JSON: ["workflow", "action:send-email"]
    metadata TEXT       -- JSON: additional info
);

CREATE INDEX idx_broker_nodes_heartbeat ON broker_nodes(last_heartbeat);

-- Work queue with claiming semantics
CREATE TABLE broker_queue (
    id TEXT PRIMARY KEY,
    work_type TEXT NOT NULL,        -- "workflow", "action", "service.call"
    payload BLOB,                   -- Serialized work data
    priority INTEGER DEFAULT 0,

    -- Claiming
    claimed_by TEXT REFERENCES broker_nodes(id),
    claimed_at INTEGER,
    lease_expires_at INTEGER,

    -- State machine
    status TEXT DEFAULT 'pending',  -- pending, claimed, completed, failed

    -- Timestamps
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_broker_queue_claim ON broker_queue(status, priority DESC, created_at);
CREATE INDEX idx_broker_queue_node ON broker_queue(claimed_by, lease_expires_at);
CREATE INDEX idx_broker_queue_type ON broker_queue(work_type, status);

-- Results storage
CREATE TABLE broker_results (
    work_id TEXT PRIMARY KEY REFERENCES broker_queue(id),
    success INTEGER NOT NULL,       -- 0/1
    result BLOB,                    -- Serialized result
    error TEXT,
    completed_at INTEGER NOT NULL,
    duration_ns INTEGER
);
```

### Claiming Algorithm

```
CLAIM WORK (Atomic via SQL Transaction):

BEGIN IMMEDIATE;  -- Exclusive lock

-- Find available work (unclaimed or expired lease)
SELECT id, work_type, payload, priority
FROM broker_queue
WHERE status = 'pending'
  AND work_type IN (:requested_types)
  AND (claimed_by IS NULL OR lease_expires_at < :now)
ORDER BY priority DESC, created_at ASC
LIMIT 1;

-- Claim it
UPDATE broker_queue
SET claimed_by = :node_id,
    claimed_at = :now,
    lease_expires_at = :now + :lease_duration,
    status = 'claimed',
    updated_at = :now
WHERE id = :work_id
  AND status = 'pending';  -- Re-check for race condition

COMMIT;
```

### Proof of Concept

**Location**: `playground/multi/`

```bash
cd playground/multi
go build -o multi_test
./multi_test

# Output:
# [Node 1] Starting as PARENT
# [Node 1] Opened SQLite DB with WAL mode
# [Node 2] Connected to SQLite DB
# [Node 3] Connected to SQLite DB
# [Node 1] SUCCESS: Multi-process SQLite works!
```

### Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| Write throughput | 10-50k ops/sec | WAL mode, serialized writes |
| Read throughput | 100k+ ops/sec | Parallel readers |
| Workflow rate | 1-5k/sec | ~5-10 DB ops per workflow |
| Claim latency | 1-5ms | Single transaction |
| Max nodes | ~10 | Same machine practical limit |

---

## Phase 2: Services Layer

### Overview

Phase 2 adds **Moleculer-style services** without changing the broker. Services are a higher-level abstraction that uses the same broker for coordination.

**What Phase 2 Adds**:
- Service definition and registration
- Action discovery and routing
- `broker.Call("service.action", params)` API
- Request-response pattern over broker

**Key Insight**: Services are just work items with `work_type = "service.call"`. The broker doesn't change; we add a service runtime on top.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PHASE 2 ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    SQLite Broker                         │   │
│  │                                                         │   │
│  │  Tables (extended):                                     │   │
│  │  ├── broker_nodes                                       │   │
│  │  ├── broker_queue                                       │   │
│  │  ├── broker_results                                     │   │
│  │  └── broker_services (NEW)                              │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│  ┌───────────────────────────┼─────────────────────────────┐   │
│  │                    Service Registry                      │   │
│  │                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │ users       │  │ emails      │  │ payments    │     │   │
│  │  │  .get       │  │  .send      │  │  .process   │     │   │
│  │  │  .create    │  │  .template  │  │  .refund    │     │   │
│  │  │  .update    │  │             │  │             │     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│           ┌──────────────────┼──────────────────┐              │
│           │                  │                  │              │
│           ▼                  ▼                  ▼              │
│      ┌─────────┐        ┌─────────┐        ┌─────────┐        │
│      │ Node 1  │        │ Node 2  │        │ Node 3  │        │
│      │ users   │        │ emails  │        │ payments│        │
│      │ service │        │ service │        │ service │        │
│      └─────────┘        └─────────┘        └─────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Service Definition

```go
// Defining a service
svc := gostage.Service("users").
    Version("1.0").
    Metadata(map[string]any{"team": "backend"})

// Adding actions
svc.Action("get", func(ctx gostage.Context) (any, error) {
    id := ctx.Params().Int("id")
    user, err := db.GetUser(id)
    return user, err
})

svc.Action("create", func(ctx gostage.Context) (any, error) {
    var input CreateUserInput
    if err := ctx.Params().Bind(&input); err != nil {
        return nil, err
    }
    user, err := db.CreateUser(input)
    return user, err
})

// Registering with runtime
runtime.RegisterService(svc)
```

### Service Calls

```go
// Synchronous call (blocks until response)
result, err := runtime.Call(ctx, "users.get", gostage.Params{
    "id": 123,
})

// Async call (returns immediately, poll for result)
requestID, err := runtime.CallAsync(ctx, "users.get", gostage.Params{
    "id": 123,
})
// Later...
result, err := runtime.GetResult(ctx, requestID)

// Call with options
result, err := runtime.Call(ctx, "users.create", input,
    gostage.WithTimeout(5*time.Second),
    gostage.WithRetries(3),
)
```

### Database Schema Extension

```sql
-- Service registry
CREATE TABLE broker_services (
    id TEXT PRIMARY KEY,              -- "users", "emails"
    version TEXT NOT NULL,            -- "1.0.0"
    node_id TEXT REFERENCES broker_nodes(id),
    actions TEXT NOT NULL,            -- JSON: ["get", "create", "update"]
    metadata TEXT,                    -- JSON: additional info
    registered_at INTEGER NOT NULL,
    last_seen INTEGER NOT NULL
);

CREATE INDEX idx_broker_services_name ON broker_services(id);
CREATE INDEX idx_broker_services_node ON broker_services(node_id);

-- When a node registers a service:
INSERT INTO broker_services (id, version, node_id, actions, metadata, registered_at, last_seen)
VALUES ('users', '1.0', 'node-1', '["get","create"]', '{}', :now, :now)
ON CONFLICT(id) DO UPDATE SET
    node_id = excluded.node_id,
    actions = excluded.actions,
    last_seen = excluded.last_seen;
```

### Call Flow

```
┌───────────────────────────────────────────────────────────────────┐
│                     SERVICE CALL FLOW                             │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. Caller: runtime.Call("users.get", {id: 123})                  │
│                           │                                       │
│                           ▼                                       │
│  2. Broker: INSERT INTO broker_queue                              │
│     (work_type='service.call', payload={                          │
│       service: 'users',                                           │
│       action: 'get',                                              │
│       params: {id: 123},                                          │
│       reply_to: 'request-uuid'  ◀── For response routing          │
│     })                                                            │
│                           │                                       │
│                           ▼                                       │
│  3. Worker (users service): Claims work where                     │
│     work_type='service.call' AND payload.service='users'          │
│                           │                                       │
│                           ▼                                       │
│  4. Worker: Executes users.get({id: 123})                         │
│     Returns: {name: "John", email: "john@example.com"}            │
│                           │                                       │
│                           ▼                                       │
│  5. Broker: INSERT INTO broker_results                            │
│     (work_id='request-uuid', result={...})                        │
│                           │                                       │
│                           ▼                                       │
│  6. Caller: Polls/subscribes for result with reply_to             │
│     Returns: {name: "John", email: "john@example.com"}            │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## Phase 3: NATS JetStream Distributed

### Overview

Phase 3 replaces SQLite with NATS JetStream for **multi-machine distributed deployment**.

**What Changes**:
- Broker implementation (SQLite → NATS)
- Deployment model (single machine → cluster)
- Throughput (1-5k/sec → 10-100k/sec)

**What Stays The Same**:
- Broker interface (same API)
- SDK interface (same API)
- Application code (no changes needed)

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PHASE 3 ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  NATS JetStream Cluster                  │   │
│  │                                                         │   │
│  │  ┌─────────────────────────────────────────────────┐   │   │
│  │  │ Stream: WORK                                    │   │   │
│  │  │ ├── Subject: work.pending.*                     │   │   │
│  │  │ ├── Subject: work.completed.*                   │   │   │
│  │  │ └── Consumer Group: "workers"                   │   │   │
│  │  └─────────────────────────────────────────────────┘   │   │
│  │                                                         │   │
│  │  ┌─────────────────────────────────────────────────┐   │   │
│  │  │ KV Bucket: nodes                                │   │   │
│  │  │ KV Bucket: services                             │   │   │
│  │  │ KV Bucket: results                              │   │   │
│  │  └─────────────────────────────────────────────────┘   │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│     ┌────────────────────────┼────────────────────────┐        │
│     │                        │                        │        │
│     ▼                        ▼                        ▼        │
│  ┌──────┐                ┌──────┐                ┌──────┐     │
│  │Node 1│                │Node 2│                │Node 3│     │
│  │ VM-A │                │ VM-B │                │ VM-C │     │
│  └──────┘                └──────┘                └──────┘     │
│                                                                 │
│  Multi-machine deployment with automatic work distribution     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### NATS vs SQLite Comparison

| Aspect | SQLite (Phase 1-2) | NATS JetStream (Phase 3) |
|--------|-------------------|-------------------------|
| **Model** | Pull (polling) | Push (subscription) |
| **Throughput** | 10-50k writes/sec | 100k-1M+ msg/sec |
| **Latency** | 1-10ms | 0.1-5ms |
| **Scaling** | Single machine | Multi-machine cluster |
| **Deployment** | Embedded (zero-deploy) | Separate process |
| **Replication** | None (single file) | Built-in Raft |
| **Work Distribution** | Custom SQL queries | Consumer groups |

### NATSBroker Implementation

```go
type NATSBroker struct {
    nc      *nats.Conn
    js      nats.JetStreamContext

    // KV Buckets
    nodes    nats.KeyValue
    services nats.KeyValue
    results  nats.KeyValue

    // Stream
    workStream string
}

func (b *NATSBroker) Submit(ctx context.Context, work Work) error {
    subject := fmt.Sprintf("work.pending.%s", work.Type)
    _, err := b.js.Publish(subject, encode(work), nats.Context(ctx))
    return err
}

func (b *NATSBroker) Claim(ctx context.Context, nodeID string, types []string) (*Work, error) {
    // Create durable consumer for this node
    sub, err := b.js.PullSubscribe(
        "work.pending.>",
        "workers",  // Consumer group
        nats.Bind(b.workStream, "workers"),
    )
    if err != nil {
        return nil, err
    }

    msgs, err := sub.Fetch(1, nats.Context(ctx))
    if err != nil || len(msgs) == 0 {
        return nil, nil  // No work available
    }

    msg := msgs[0]
    work := decode(msg.Data)

    // Mark as in-progress (NATS handles this via ack window)
    msg.InProgress()

    return work, nil
}

func (b *NATSBroker) Complete(ctx context.Context, workID string, result Result) error {
    // Store result in KV
    _, err := b.results.Put(workID, encode(result))
    if err != nil {
        return err
    }

    // Publish completion event
    b.js.Publish("work.completed."+workID, encode(result))
    return nil
}
```

### Migration from SQLite to NATS

```go
// Configuration-driven broker selection
func NewBroker(cfg Config) (Broker, error) {
    switch cfg.BrokerType {
    case "sqlite":
        return NewSQLiteBroker(cfg.SQLite.Path, cfg.SQLite.Options...)
    case "nats":
        return NewNATSBroker(cfg.NATS.URL, cfg.NATS.Options...)
    default:
        return NewSQLiteBroker("./gostage.db")  // Default to SQLite
    }
}

// Application code unchanged!
runtime, _ := gostage.New(
    gostage.WithBroker(broker),  // SQLite or NATS
)
runtime.RegisterWorkflow("send-email", emailWorkflow)
runtime.Start(ctx)
```

---

## SDK Design

### Overview

The SDK provides **lightweight access** to the broker for external clients that don't need to run a full GoStage runtime.

**Use Cases**:
- HTTP API server submitting workflows
- CLI tools for work submission
- Cron jobs triggering workflows
- External services calling GoStage services

### Interface Design

```go
package gostage

// Broker provides work submission and coordination.
// The broker IS the client - no separate SDK type needed.
//
// Usage follows standard Go patterns:
//   broker, _ := gostage.OpenSQLite("./workflow.db")
//   defer broker.Close()
//   id, _ := broker.Submit(ctx, "send-email", input)
type Broker interface {
    // ==================== Workflow Operations ====================

    // Submit submits a workflow for execution.
    // Returns immediately with workflow ID.
    Submit(ctx context.Context, name string, input any, opts ...SubmitOption) (WorkflowID, error)

    // Wait blocks until workflow completes and returns result.
    Wait(ctx context.Context, id WorkflowID, opts ...WaitOption) (Result, error)

    // Cancel cancels a running or pending workflow.
    Cancel(ctx context.Context, id WorkflowID) error

    // Status returns current workflow status.
    Status(ctx context.Context, id WorkflowID) (Status, error)

    // ==================== Service Operations (Phase 2+) ====================

    // Call invokes a service action and waits for response.
    Call(ctx context.Context, action string, params any, opts ...CallOption) (any, error)

    // CallAsync invokes a service action without waiting.
    CallAsync(ctx context.Context, action string, params any, opts ...CallOption) (RequestID, error)

    // ==================== Generic Work Operations ====================

    // SubmitWork submits generic work to the broker.
    SubmitWork(ctx context.Context, workType string, payload any, opts ...SubmitOption) (WorkID, error)

    // Result retrieves result for a completed work item.
    Result(ctx context.Context, id WorkID) (Result, error)

    // ==================== Health & Lifecycle ====================

    // Ping checks broker connectivity.
    Ping(ctx context.Context) error

    // Close releases broker resources.
    Close() error
}

// ==================== Broker Constructors ====================

// OpenSQLite opens a SQLite-backed broker.
// Use for same-machine deployment (Phase 1-2).
//
// Usage:
//   broker, _ := gostage.OpenSQLite("./workflow.db")
//   defer broker.Close()
func OpenSQLite(path string, opts ...SQLiteOption) (Broker, error)

// OpenNATS opens a NATS-backed broker.
// Use for distributed deployment (Phase 3).
//
// Usage:
//   broker, _ := gostage.OpenNATS("nats://cluster:4222")
//   defer broker.Close()
func OpenNATS(url string, opts ...NATSOption) (Broker, error)

// ==================== Options ====================

type SubmitOption func(*submitConfig)

func WithPriority(p Priority) SubmitOption
func WithTags(tags ...string) SubmitOption
func WithMetadata(m map[string]any) SubmitOption
func WithIdempotencyKey(key string) SubmitOption

type WaitOption func(*waitConfig)

func WithPollingInterval(d time.Duration) WaitOption

type CallOption func(*callConfig)

func WithTimeout(d time.Duration) CallOption
func WithRetries(n int) CallOption
func WithFallback(action string) CallOption
```

### Usage Examples

```go
// ==================== Phase 1: SQLite ====================

// Open SQLite broker
broker, _ := gostage.OpenSQLite("./workflow.db")
defer broker.Close()

// Submit workflow
id, _ := broker.Submit(ctx, "send-email", EmailInput{
    To:      "user@example.com",
    Subject: "Welcome!",
    Body:    "Hello...",
})

// Wait for result
result, _ := broker.Wait(ctx, id)
fmt.Printf("Workflow completed: %v\n", result.Success)

// ==================== Phase 2: Service Calls ====================

// Call a service action
user, _ := broker.Call(ctx, "users.get", gostage.Params{"id": 123})
fmt.Printf("User: %v\n", user)

// Create user via service
newUser, _ := broker.Call(ctx, "users.create", CreateUserInput{
    Name:  "John Doe",
    Email: "john@example.com",
})

// ==================== Phase 3: NATS (same API!) ====================

// Just change the opener
broker, _ := gostage.OpenNATS("nats://cluster.example.com:4222")
defer broker.Close()

// All the same operations work!
id, _ := broker.Submit(ctx, "send-email", input)
result, _ := broker.Wait(ctx, id)
user, _ := broker.Call(ctx, "users.get", params)
```

### File Structure

```
shared/
└── broker/
    ├── broker.go           # Broker interface
    ├── options.go          # Option types
    └── errors.go           # Broker-specific errors

adapters/
└── broker/
    ├── sqlite/
    │   ├── sqlite.go           # OpenSQLite implementation
    │   ├── schema.go           # Table creation
    │   └── queries.go          # SQL queries
    └── nats/
        ├── nats.go             # OpenNATS implementation
        ├── streams.go          # Stream configuration
        └── kv.go               # KV bucket operations
```

---

## Boundaries Integration

### Current Boundaries

```
boundaries/
├── foundation.go      # Clock, Errors, ID, Metadata, Locks
├── domain.go          # Queue, Store, Writer, StateReader
├── execution.go       # Runtime, Runner, Context, Actions
├── orchestration.go   # Dispatcher, Coordinator, Scheduler
├── infrastructure.go  # Bridge, Telemetry, Diagnostics, Node
├── public.go          # Public API
└── types/             # Shared types
```

### New Additions

#### 1. BrokerService in Domain Layer

Add to `boundaries/domain.go`:

```go
// DomainLayer defines what the domain layer exposes.
type DomainLayer interface {
    Queue() QueueService           // existing
    Store() StoreService           // existing
    Writer() WriterService         // existing
    StateReader() StateReaderService // existing
    Broker() BrokerService         // NEW: generic work distribution
}

// BrokerService provides work distribution operations.
//
// Purpose:
//   BrokerService is the core coordination mechanism for GoStage.
//   It abstracts work submission, claiming, and completion across
//   different backends (SQLite for Phase 1-2, NATS for Phase 3).
//
// Implementations:
//   - SQLiteBroker: Uses SQLite tables for coordination (Phase 1-2)
//   - NATSBroker: Uses NATS JetStream for coordination (Phase 3)
//
// Usage:
//   broker := domainLayer.Broker()
//   err := broker.Submit(ctx, Work{Type: "workflow", Payload: def})
//   work, err := broker.Claim(ctx, nodeID, []string{"workflow"})
type BrokerService interface {
    // ==================== Node Management ====================

    // RegisterNode registers a node with the broker.
    RegisterNode(ctx context.Context, node types.NodeInfo) error

    // Heartbeat updates node's last-seen timestamp.
    Heartbeat(ctx context.Context, nodeID string) error

    // DeregisterNode removes a node from the broker.
    DeregisterNode(ctx context.Context, nodeID string) error

    // ListNodes returns all registered nodes.
    ListNodes(ctx context.Context) ([]types.NodeInfo, error)

    // ==================== Work Management ====================

    // Submit adds work to the broker queue.
    Submit(ctx context.Context, work types.Work) error

    // Claim claims available work for a node.
    // Returns nil if no work available.
    Claim(ctx context.Context, nodeID string, workTypes []string) (*types.Work, error)

    // Complete marks work as successfully completed.
    Complete(ctx context.Context, workID string, result types.Result) error

    // Fail marks work as failed, optionally returning to queue.
    Fail(ctx context.Context, workID string, err error, requeue bool) error

    // Release releases claimed work back to pending state.
    Release(ctx context.Context, workID string) error

    // ==================== Service Discovery (Phase 2+) ====================

    // RegisterService registers a service with the broker.
    RegisterService(ctx context.Context, svc types.ServiceInfo) error

    // DeregisterService removes a service from the broker.
    DeregisterService(ctx context.Context, name string, nodeID string) error

    // DiscoverService finds nodes providing a service.
    DiscoverService(ctx context.Context, name string) ([]types.ServiceInfo, error)

    // ==================== Result Management ====================

    // GetResult retrieves result for completed work.
    GetResult(ctx context.Context, workID string) (*types.Result, error)

    // WaitResult waits for work to complete and returns result.
    WaitResult(ctx context.Context, workID string) (*types.Result, error)
}
```

#### 2. New Types in boundaries/types/

Create `boundaries/types/broker.go`:

```go
package types

import "time"

// NodeInfo represents a registered worker node.
type NodeInfo struct {
    ID           string
    StartedAt    time.Time
    LastSeen     time.Time
    Capabilities []string          // Work types this node handles
    Metadata     map[string]any
}

// Work represents a unit of work in the broker queue.
type Work struct {
    ID        string
    Type      string              // "workflow", "action", "service.call"
    Payload   []byte              // Serialized work data
    Priority  Priority
    CreatedAt time.Time

    // For service calls
    ReplyTo   string              // Request ID for response routing
}

// Result represents the outcome of completed work.
type Result struct {
    WorkID      string
    Success     bool
    Data        []byte              // Serialized result
    Error       string
    CompletedAt time.Time
    Duration    time.Duration
}

// ServiceInfo represents a registered service.
type ServiceInfo struct {
    Name        string
    Version     string
    NodeID      string
    Actions     []string
    Metadata    map[string]any
    RegisteredAt time.Time
    LastSeen    time.Time
}

// ServiceCall represents a service invocation request.
type ServiceCall struct {
    Service   string
    Action    string
    Params    []byte              // Serialized params
    ReplyTo   string              // For response routing
    Timeout   time.Duration
}
```

#### 3. Broker in Public Layer

The `Broker` interface lives in `shared/broker/` and is the primary external API. The public layer exposes it via the runtime:

```go
// In gostage.go (package root)

// OpenSQLite opens a SQLite-backed broker for external clients.
func OpenSQLite(path string, opts ...SQLiteOption) (Broker, error) {
    return sqlite.Open(path, opts...)
}

// OpenNATS opens a NATS-backed broker for external clients.
func OpenNATS(url string, opts ...NATSOption) (Broker, error) {
    return nats.Open(url, opts...)
}

// Runtime also exposes broker for internal use
type Runtime interface {
    // ... existing methods ...

    Broker() Broker  // Access to broker for programmatic submission
}
```

### Relationship to Existing QueueService

The existing `QueueService` in domain.go is **workflow-specific**. It will internally use `BrokerService`:

```go
// QueueService (existing) - workflow-specific
type QueueService interface {
    Enqueue(ctx, defBytes, priority, metadata) (WorkflowID, error)
    Claim(ctx, selector, workerID) (*ClaimedWorkflow, error)
    Ack(ctx, id, summary) error
    // ...
}

// BrokerService (new) - generic work distribution
type BrokerService interface {
    Submit(ctx, work) error
    Claim(ctx, nodeID, workTypes) (*Work, error)
    Complete(ctx, workID, result) error
    // ...
}

// QueueService implementation uses BrokerService internally:
func (q *queueImpl) Enqueue(ctx context.Context, ...) (WorkflowID, error) {
    work := Work{
        Type:    "workflow",
        Payload: defBytes,
        // ...
    }
    return q.broker.Submit(ctx, work)
}
```

---

## Migration Plan

### Phase 1 Migration Steps

#### Step 1.1: Add Broker Types
```bash
# Create broker types
boundaries/types/broker.go
```

**Files Changed**:
- `boundaries/types/broker.go` (new)

**Duration**: 1-2 hours

#### Step 1.2: Add BrokerService Interface
```bash
# Add to domain boundary
boundaries/domain.go
```

**Changes**:
- Add `Broker() BrokerService` to `DomainLayer`
- Add `BrokerService` interface definition

**Duration**: 2-3 hours

#### Step 1.3: Create SQLiteBroker Implementation
```bash
# Create implementation
internal/domain/broker/sqlite_broker.go
internal/domain/broker/schema.sql
internal/domain/broker/queries.go
```

**Duration**: 1-2 days

#### Step 1.4: Refactor QueueService
```bash
# Update to use BrokerService internally
internal/domain/queue/queue.go
```

**Changes**:
- QueueService wraps BrokerService for workflow-specific operations
- Backward compatible - existing API unchanged

**Duration**: 1 day

#### Step 1.5: Create Broker Package
```bash
# Create external Broker interface
shared/broker/broker.go
shared/broker/options.go
adapters/broker/sqlite/sqlite.go  # OpenSQLite
```

**Duration**: 1-2 days

#### Step 1.6: Wire to Package Root
```bash
# Expose OpenSQLite from gostage package
gostage.go  # Add OpenSQLite function
```

**Duration**: 1 day

### Phase 2 Migration Steps

#### Step 2.1: Add Service Types
```bash
# Add to broker types
boundaries/types/broker.go  # Add ServiceInfo, ServiceCall
```

**Duration**: 2-3 hours

#### Step 2.2: Extend BrokerService for Services
```bash
# Add service methods to interface
boundaries/domain.go  # Add RegisterService, DiscoverService
```

**Duration**: 2-3 hours

#### Step 2.3: Extend SQLiteBroker for Services
```bash
# Add service tables and queries
internal/domain/broker/sqlite_broker.go
internal/domain/broker/service_queries.go
```

**Duration**: 1 day

#### Step 2.4: Create Service Runtime
```bash
# Create service layer
internal/execution/service/service.go
internal/execution/service/registry.go
internal/execution/service/call.go
```

**Duration**: 2-3 days

#### Step 2.5: Add Service Methods to Broker
```bash
# Add Call, CallAsync to Broker interface
shared/broker/broker.go
adapters/broker/sqlite/sqlite.go
```

**Duration**: 1 day

### Phase 3 Migration Steps

#### Step 3.1: Create NATSBroker Implementation
```bash
# Create NATS implementation
adapters/broker/nats/nats_broker.go
adapters/broker/nats/streams.go
adapters/broker/nats/kv.go
```

**Implements same BrokerService interface as SQLiteBroker**

**Duration**: 2-3 days

#### Step 3.2: Add Broker Factory
```bash
# Configuration-driven broker selection
shared/broker/factory.go
```

**Duration**: 1 day

#### Step 3.3: Update SDK for NATS
```bash
# Add NATS adapter
adapters/broker/nats/sdk_adapter.go
```

**Duration**: 1 day

#### Step 3.4: Multi-Machine Testing
```bash
# Integration tests
e2e/distributed_test.go
```

**Duration**: 2-3 days

---

## Implementation Timeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    IMPLEMENTATION TIMELINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PHASE 1: SQLite Broker + Workflows                             │
│  ─────────────────────────────────────────────────────────────  │
│  Week 1-2:                                                      │
│    ├── Step 1.1: Add Broker Types                               │
│    ├── Step 1.2: Add BrokerService Interface                    │
│    └── Step 1.3: Create SQLiteBroker Implementation             │
│                                                                 │
│  Week 3:                                                        │
│    ├── Step 1.4: Refactor QueueService                          │
│    ├── Step 1.5: Create SDK Package                             │
│    └── Step 1.6: Add SDK to Public Layer                        │
│                                                                 │
│  Week 4: Testing + Documentation                                │
│                                                                 │
│  PHASE 2: Services Layer                                        │
│  ─────────────────────────────────────────────────────────────  │
│  Week 5-6:                                                      │
│    ├── Step 2.1: Add Service Types                              │
│    ├── Step 2.2: Extend BrokerService                           │
│    ├── Step 2.3: Extend SQLiteBroker                            │
│    └── Step 2.4: Create Service Runtime                         │
│                                                                 │
│  Week 7:                                                        │
│    ├── Step 2.5: Add Service to SDK                             │
│    └── Testing + Documentation                                  │
│                                                                 │
│  PHASE 3: NATS Distributed                                      │
│  ─────────────────────────────────────────────────────────────  │
│  Week 8-9:                                                      │
│    ├── Step 3.1: Create NATSBroker                              │
│    ├── Step 3.2: Add Broker Factory                             │
│    └── Step 3.3: Update SDK for NATS                            │
│                                                                 │
│  Week 10:                                                       │
│    ├── Step 3.4: Multi-Machine Testing                          │
│    └── Documentation + Examples                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Success Metrics

### Phase 1 Success Criteria

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Multi-process coordination | Works | `playground/multi` test passes |
| SDK workflow submission | Works | Submit workflow via SDK, verify execution |
| Throughput | ≥1k workflows/sec | Benchmark test |
| Backward compatibility | 100% | Existing tests pass |

### Phase 2 Success Criteria

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Service registration | Works | Register service, verify in broker_services |
| Service discovery | Works | Discover service by name |
| Service calls | Works | Call("service.action") returns result |
| Throughput | ≥1k calls/sec | Benchmark test |

### Phase 3 Success Criteria

| Metric | Target | How to Measure |
|--------|--------|----------------|
| NATS integration | Works | Submit/Claim via NATSBroker |
| Multi-machine | Works | Nodes on different machines coordinate |
| Throughput | ≥10k/sec | Benchmark with NATS cluster |
| API compatibility | 100% | Same tests pass with NATS backend |

### Overall Success Criteria

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Code change for backend swap | <5% | Lines changed in application code |
| Test coverage | ≥80% | Coverage report |
| Documentation completeness | 100% | All public APIs documented |
| Examples for each phase | ≥3 | Working examples in examples/ |

---

## Risk Assessment

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| SQLite contention under load | Medium | Medium | Optimize queries, connection pooling |
| NATS learning curve | Medium | Low | Start with simple patterns, reference docs |
| Service discovery complexity | Low | Medium | Keep registry simple, add features later |
| Migration breaks existing code | Low | High | Comprehensive tests, backward compatibility |

### Schedule Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Phase 1 takes longer | Medium | Medium | Phase 1 is foundation, worth investing time |
| Phase 2 scope creep | High | Medium | Keep service layer minimal initially |
| Phase 3 NATS complexity | Medium | Low | Phase 3 is optional, can delay if needed |

### Mitigation Strategies

1. **Start with proven patterns**: SQLite multi-process (proven in playground/multi)
2. **Interface-first design**: Define Broker interface before implementation
3. **Incremental migration**: One step at a time, validate at each step
4. **Backward compatibility**: Never break existing QueueService API
5. **Comprehensive testing**: Unit, integration, and e2e tests at each phase

---

## Appendix: Quick Reference

### Broker Interface Quick Reference (External API)

```go
// External clients use this interface
type Broker interface {
    Submit(ctx, name, input, opts...) (WorkflowID, error)
    Wait(ctx, id, opts...) (Result, error)
    Cancel(ctx, id) error
    Status(ctx, id) (Status, error)
    Call(ctx, action, params, opts...) (any, error)  // Phase 2+
    Ping(ctx) error
    Close() error
}
```

### BrokerService Quick Reference (Internal)

```go
// Internal coordination (used by runtime)
type BrokerService interface {
    RegisterNode(ctx, node) error
    Heartbeat(ctx, nodeID) error
    Submit(ctx, work) error
    Claim(ctx, nodeID, types) (*Work, error)
    Complete(ctx, workID, result) error
    Fail(ctx, workID, err, requeue) error
    RegisterService(ctx, svc) error  // Phase 2+
}
```

### Usage Quick Reference

```go
// SQLite (Phase 1-2)
broker, _ := gostage.OpenSQLite("./workflow.db")
defer broker.Close()

// NATS (Phase 3)
broker, _ := gostage.OpenNATS("nats://cluster:4222")
defer broker.Close()

// Operations (same for both!)
id, _ := broker.Submit(ctx, "workflow-name", input)
result, _ := broker.Wait(ctx, id)
user, _ := broker.Call(ctx, "users.get", params)  // Phase 2+
```

### File Locations Quick Reference

```
boundaries/
├── domain.go           # Add BrokerService (internal)
└── types/broker.go     # Broker types

shared/
└── broker/
    └── broker.go       # Broker interface (external API)

adapters/
└── broker/
    ├── sqlite/
    │   └── sqlite.go   # OpenSQLite
    └── nats/           # Phase 3
        └── nats.go     # OpenNATS
```

---

## Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Nov 2025 | Initial comprehensive plan |
