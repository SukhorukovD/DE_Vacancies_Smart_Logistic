BEGIN;

CREATE SCHEMA IF NOT EXISTS core_stage;

CREATE TABLE IF NOT EXISTS audit.core_task_runs (
    raw_batch_id uuid NOT NULL
        REFERENCES audit.raw_to_core_runs(raw_batch_id),
    task_name text NOT NULL,
    status text NOT NULL
        CHECK (status IN ('pending', 'success', 'failed')),
    finished_at timestamptz,
    summary jsonb,
    error_message text,
    PRIMARY KEY (raw_batch_id, task_name)
);

CREATE TABLE IF NOT EXISTS core_stage.active_run (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    raw_batch_id uuid NOT NULL
        REFERENCES audit.raw_to_core_runs(raw_batch_id),
    owner_run_id text NOT NULL
);

COMMIT;