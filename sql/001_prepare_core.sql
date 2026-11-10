BEGIN;

ALTER TABLE core.prices
    ADD COLUMN IF NOT EXISTS price_rub numeric;

ALTER TABLE core.prices
    ADD COLUMN IF NOT EXISTS applied_rate numeric;

ALTER TABLE core.cities
    ADD COLUMN IF NOT EXISTS region_name text;

ALTER TABLE core.price_body_types
    ADD COLUMN IF NOT EXISTS source_kind text NOT NULL DEFAULT 'link';

CREATE TABLE IF NOT EXISTS audit.raw_to_core_runs (
    raw_batch_id uuid PRIMARY KEY
        REFERENCES audit.etl_batches(batch_id),
    source_started_at timestamptz NOT NULL,
    status text NOT NULL
        CHECK (status IN ('running', 'success', 'failed')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    error_message text,
    summary jsonb
);

ALTER TABLE audit.raw_to_core_runs
    ADD COLUMN IF NOT EXISTS pipeline_version text NOT NULL DEFAULT 'legacy';

CREATE TABLE IF NOT EXISTS audit.raw_to_core_results (
    run_batch_id uuid NOT NULL
        REFERENCES audit.raw_to_core_runs(raw_batch_id),
    source_table text NOT NULL,
    source_id bigint NOT NULL,
    source_batch_id uuid NOT NULL,
    record_hash text NOT NULL,
    outcome text NOT NULL CHECK (
        outcome IN (
            'accepted',
            'rejected',
            'deduplicated',
            'validated_only'
        )
    ),
    error_codes text[] NOT NULL,
    PRIMARY KEY (run_batch_id, source_table, source_id)
);

CREATE TABLE IF NOT EXISTS core.subscriber_directions (
    subscriber_id bigint NOT NULL,
    loading_city_gcid integer NOT NULL
        REFERENCES core.cities(gc_id),
    unloading_city_gcid integer NOT NULL
        REFERENCES core.cities(gc_id),
    body_type_std_id integer NOT NULL
        REFERENCES core.body_types(std_id),
    is_favorite boolean NOT NULL,
    is_selected boolean NOT NULL,
    batch_id uuid NOT NULL,
    PRIMARY KEY (
        subscriber_id,
        loading_city_gcid,
        unloading_city_gcid,
        body_type_std_id
    ),
    CHECK (is_favorite OR is_selected)
);

CREATE OR REPLACE VIEW core.v_prices_analytics AS
WITH bodies AS (
    SELECT
        b.price_sk,
        array_agg(t.std_id ORDER BY t.std_id) AS body_group_ids,
        array_agg(t.canonical_name ORDER BY t.std_id) AS body_groups,
        array_agg(
            DISTINCT b.source_kind ORDER BY b.source_kind
        ) AS body_sources
    FROM core.price_body_types b
    JOIN core.body_types t
        ON t.std_id = b.body_type_std_id
    GROUP BY b.price_sk
),
dates AS (
    SELECT
        price_sk,
        array_agg(price_date ORDER BY price_date) AS price_dates,
        array_agg(
            DISTINCT date_trunc('month', price_date)::date
            ORDER BY date_trunc('month', price_date)::date
        ) AS price_months
    FROM core.price_dates
    GROUP BY price_sk
),
participants AS (
    SELECT
        price_sk,
        array_agg(subscriber_id ORDER BY subscriber_id) AS participant_ids
    FROM core.price_participants
    GROUP BY price_sk
)
SELECT
    p.price_sk,
    p.source_id AS price_id,
    p.loading_date,
    p.end_loading_date,
    p.loading_city_gcid,
    lc.name AS loading_city,
    lc.region_name AS loading_region,
    lc.latitude AS loading_latitude,
    lc.longitude AS loading_longitude,
    p.unloading_city_gcid,
    uc.name AS unloading_city,
    uc.region_name AS unloading_region,
    uc.latitude AS unloading_latitude,
    uc.longitude AS unloading_longitude,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.weight_category,
    p.volume_category,
    coalesce(b.body_group_ids, ARRAY[-1]) AS body_group_ids,
    coalesce(
        b.body_groups,
        ARRAY['Не указан']::varchar[]
    ) AS body_groups,
    coalesce(b.body_sources, ARRAY['missing']) AS body_sources,
    coalesce(d.price_dates, ARRAY[]::date[]) AS price_dates,
    coalesce(d.price_months, ARRAY[]::date[]) AS price_months,
    coalesce(a.participant_ids, ARRAY[]::bigint[]) AS participant_ids,
    p.subscriber_id,
    p.organization_id,
    p.price AS source_price,
    p.currency_code,
    p.nearest_rate_date,
    p.applied_rate,
    p.price_rub,
    p.price_rub / nullif(p.distance::numeric, 0) AS price_rub_per_km,
    p.price_type,
    p.is_fact,
    p.complex,
    p.outlier_mark,
    p.observed_from,
    p.batch_id
FROM core.prices p
JOIN core.cities lc
    ON lc.gc_id = p.loading_city_gcid
JOIN core.cities uc
    ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN bodies b
    ON b.price_sk = p.price_sk
LEFT JOIN dates d
    ON d.price_sk = p.price_sk
LEFT JOIN participants a
    ON a.price_sk = p.price_sk
WHERE p.is_current;

COMMIT;