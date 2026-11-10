\set ON_ERROR_STOP on

BEGIN;

SET LOCAL lock_timeout = '10s';

DO $$
BEGIN
    IF NOT pg_try_advisory_xact_lock(73021, 1) THEN
        RAISE EXCEPTION
            'RAW -> CORE is running; wait for it to finish';
    END IF;
END $$;

-- CASCADE не используется: внешние зависимости должны блокировать очистку.
TRUNCATE TABLE
    core.price_body_types,
    core.price_dates,
    core.price_participants,
    core.price_group_stats,
    core.subscriber_directions,
    core.prices,
    core.exchange_rates,
    core.city_id_mapping,
    core.body_type_id_mapping,
    core.cities,
    core.body_types,
    core_stage.active_run,
    audit.core_task_runs,
    quarantine.invalid_records,
    audit.raw_to_core_results,
    audit.raw_to_core_runs
RESTART IDENTITY;

COMMIT;