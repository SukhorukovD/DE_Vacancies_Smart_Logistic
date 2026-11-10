-- Где физически находятся RAW-цены и какие батчи
-- проходят условия отбора текущего ETL.

WITH required(name) AS (
    VALUES
        ('cities'),
        ('body_types'),
        ('exchange_rates'),
        ('prices'),
        ('price_dates'),
        ('price_body_types'),
        ('price_participants'),
        ('price_group_stats'),
        ('favorite_directions'),
        ('selected_directions'),
        ('migrations')
),
physical AS (
    SELECT _batch_id AS batch_id, count(*) AS raw_price_versions
    FROM raw.prices
    GROUP BY _batch_id
),
target AS (
    SELECT batch_id, started_at
    FROM audit.etl_batches
    WHERE batch_id = '08c00df7-d086-4341-9b1c-b7bf891f9d77'
)
SELECT
    coalesce(b.batch_id, p.batch_id) AS raw_batch_id,
    b.status AS raw_batch_status,
    b.started_at,
    coalesce(p.raw_price_versions, 0) AS raw_price_versions,
    (
        SELECT (b.started_at, b.batch_id)
            <= (t.started_at, t.batch_id)
        FROM target t
    ) AS within_target_history,
    ARRAY(
        SELECT r.name
        FROM required r
        WHERE NOT EXISTS (
            SELECT 1
            FROM audit.etl_table_runs tr
            WHERE tr.batch_id = coalesce(b.batch_id, p.batch_id)
              AND tr.source_table = r.name
              AND tr.status = 'success'
        )
        ORDER BY r.name
    ) AS missing_successful_tasks
FROM audit.etl_batches b
FULL JOIN physical p ON p.batch_id = b.batch_id
WHERE p.batch_id IS NOT NULL
   OR b.batch_id = '08c00df7-d086-4341-9b1c-b7bf891f9d77'
ORDER BY b.started_at NULLS LAST;


-- Состояние сохранённых версий CORE:
-- есть ли нормализованная цена и когда закрылись версии.

SELECT
    batch_id AS version_created_by_batch,
    observed_to,
    is_current,
    count(*) AS versions,
    count(*) FILTER (
        WHERE price_rub > 0
    ) AS with_positive_price_rub,
    count(*) FILTER (
        WHERE EXISTS (
            SELECT 1
            FROM core.price_dates d
            WHERE d.price_sk = p.price_sk
        )
    ) AS with_dates,
    count(*) FILTER (
        WHERE EXISTS (
            SELECT 1
            FROM core.price_body_types b
            WHERE b.price_sk = p.price_sk
        )
    ) AS with_body_groups
FROM core.prices p
GROUP BY batch_id, observed_to, is_current
ORDER BY observed_to DESC NULLS FIRST;