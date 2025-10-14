-- +migrate Up
CREATE TABLE IF NOT EXISTS queue_entries (
    id TEXT PRIMARY KEY,
    definition BLOB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    claimed_by TEXT,
    claimed_at DATETIME,
    lease_id TEXT,
    metadata BLOB
);

CREATE INDEX IF NOT EXISTS idx_queue_state_priority ON queue_entries(state, priority DESC, created_at);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    type TEXT,
    tags BLOB,
    metadata BLOB,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    completed_at DATETIME,
    duration INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    success INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS stage_runs (
    workflow_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    name TEXT,
    tags BLOB,
    dynamic INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    completed_at DATETIME,
    state TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (workflow_id, stage_id),
    FOREIGN KEY (workflow_id) REFERENCES workflow_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS action_runs (
    workflow_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    ref TEXT,
    tags BLOB,
    dynamic INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME,
    completed_at DATETIME,
    state TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (workflow_id, stage_id, action_id),
    FOREIGN KEY (workflow_id, stage_id) REFERENCES stage_runs(workflow_id, stage_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS execution_summaries (
    workflow_id TEXT PRIMARY KEY,
    final_store BLOB,
    disabled_stages BLOB,
    disabled_actions BLOB,
    removed_stages BLOB,
    removed_actions BLOB,
    FOREIGN KEY (workflow_id) REFERENCES workflow_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS telemetry_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT,
    stage_id TEXT,
    action_id TEXT,
    kind TEXT NOT NULL,
    attempt INTEGER,
    occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message TEXT,
    metadata BLOB,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_telemetry_workflow ON telemetry_events(workflow_id, occurred_at);
