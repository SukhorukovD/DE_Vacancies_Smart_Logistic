\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM core.prices
        WHERE is_current
          AND (
              price_rub IS NULL OR price_rub <= 0
              OR applied_rate IS NULL OR applied_rate <= 0
          )
    ) THEN
        RAISE EXCEPTION 'Current prices have invalid normalized values';
    END IF;

    IF (SELECT count(*) FROM core.v_prices_analytics)
       <> (SELECT count(*) FROM core.prices WHERE is_current)
    THEN
        RAISE EXCEPTION 'Analytics view lost or multiplied prices';
    END IF;

    IF (SELECT sum(price_rub) FROM core.v_prices_analytics)
       IS DISTINCT FROM (
           SELECT sum(price_rub)
           FROM core.prices
           WHERE is_current
       )
    THEN
        RAISE EXCEPTION 'Analytics view changed the price sum';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM core.prices p
        WHERE p.is_current
          AND NOT EXISTS (
              SELECT 1
              FROM core.price_body_types b
              WHERE b.price_sk = p.price_sk
          )
    ) THEN
        RAISE EXCEPTION 'Current price has no body group';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (
            SELECT
                source_id,
                observed_from,
                observed_to,
                is_current,
                lead(observed_from) OVER (
                    PARTITION BY source_id
                    ORDER BY observed_from, price_sk
                ) AS next_start
            FROM core.prices
        ) h
        WHERE observed_to <= observed_from
           OR is_current IS DISTINCT FROM (observed_to IS NULL)
           OR (
               next_start IS NOT NULL
               AND (
                   observed_to IS NULL
                   OR observed_to > next_start
               )
           )
    ) THEN
        RAISE EXCEPTION 'Invalid price SCD2 intervals';
    END IF;
END $$;

SELECT raw_batch_id, status, pipeline_version, summary
FROM audit.raw_to_core_runs
ORDER BY source_started_at;

SELECT
    run_batch_id,
    source_table,
    outcome,
    count(*) AS rows_count
FROM audit.raw_to_core_results
GROUP BY run_batch_id, source_table, outcome
ORDER BY run_batch_id, source_table, outcome;

COMMIT;