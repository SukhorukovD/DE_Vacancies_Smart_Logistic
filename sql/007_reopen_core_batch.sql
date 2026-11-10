\set ON_ERROR_STOP on

BEGIN;
SET LOCAL lock_timeout = '10s';

DO $$
DECLARE
    target uuid := '08c00df7-d086-4341-9b1c-b7bf891f9d77';
    v_repair_key text := 'empty_snapshot_08c00df7_v1';
BEGIN
    IF NOT pg_try_advisory_xact_lock(73021, 1) THEN
        RAISE EXCEPTION
            'CORE or MART is running; wait for completion';
    END IF;

    IF EXISTS (SELECT 1 FROM core_stage.active_run) THEN
        RAISE EXCEPTION
            'An unfinished CORE run exists; recovery stopped';
    END IF;

    IF EXISTS (SELECT 1 FROM core.prices WHERE is_current) THEN
        RAISE EXCEPTION
            'Current CORE prices already exist; recovery stopped';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM audit.raw_to_core_runs
        WHERE raw_batch_id = target
          AND status = 'success'
          AND pipeline_version = 'core_v2'
          AND summary->'prices'->>'read' = '0'
    ) THEN
        RAISE EXCEPTION
            'Expected failed-empty-success batch not found';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM audit.core_task_runs
        WHERE raw_batch_id = target
          AND task_name = 'prices'
          AND summary->>'read' = '0'
          AND (summary->'scd2'->>'closed_versions')::bigint > 0
          AND summary->'scd2'->>'inserted_versions' = '0'
    ) THEN
        RAISE EXCEPTION
            'Task audit does not match the diagnosed incident';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM audit.raw_to_core_runs r
        JOIN audit.etl_batches b
            ON b.batch_id = r.raw_batch_id
        JOIN audit.etl_batches t
            ON t.batch_id = target
        WHERE r.status = 'success'
          AND (b.started_at, b.batch_id)
              > (t.started_at, t.batch_id)
    ) THEN
        RAISE EXCEPTION
            'A later CORE batch succeeded; recovery stopped';
    END IF;

    CREATE TABLE IF NOT EXISTS audit.core_repair_backup (
        repair_key text NOT NULL,
        entity text NOT NULL,
        row_data jsonb NOT NULL,
        backed_up_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );

    IF EXISTS (
        SELECT 1
        FROM audit.core_repair_backup b
        WHERE b.repair_key = v_repair_key
    ) THEN
        RAISE EXCEPTION
            'Recovery backup already exists; do not repeat this SQL';
    END IF;

    INSERT INTO audit.core_repair_backup (
        repair_key, entity, row_data
    )
    SELECT
        v_repair_key,
        'raw_to_core_runs',
        to_jsonb(r)
    FROM audit.raw_to_core_runs r
    WHERE r.raw_batch_id = target;

    INSERT INTO audit.core_repair_backup (
        repair_key, entity, row_data
    )
    SELECT
        v_repair_key,
        'core_task_runs',
        to_jsonb(r)
    FROM audit.core_task_runs r
    WHERE r.raw_batch_id = target;

    INSERT INTO audit.core_repair_backup (
        repair_key, entity, row_data
    )
    SELECT
        v_repair_key,
        'raw_to_core_results',
        to_jsonb(r)
    FROM audit.raw_to_core_results r
    WHERE r.run_batch_id = target;

    DELETE FROM audit.raw_to_core_results
    WHERE run_batch_id = target;

    DELETE FROM audit.core_task_runs
    WHERE raw_batch_id = target;

    UPDATE audit.raw_to_core_runs
    SET
        status = 'failed',
        error_message =
            'Reopened after empty snapshot bug; audit saved in core_repair_backup'
    WHERE raw_batch_id = target;
END $$;

COMMIT;