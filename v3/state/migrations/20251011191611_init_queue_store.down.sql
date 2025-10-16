-- +migrate Down
DROP TABLE IF EXISTS telemetry_events;
DROP TABLE IF EXISTS execution_summaries;
DROP TABLE IF EXISTS action_runs;
DROP TABLE IF EXISTS stage_runs;
DROP TABLE IF EXISTS workflow_runs;
DROP TABLE IF EXISTS queue_entry_tags;
DROP TABLE IF EXISTS queue_entries;
