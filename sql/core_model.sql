-- Base schema used by the current RAW -> CORE pipeline (SCD2 prices/rates).
-- Safe on an already initialized compatible DWH; never resets existing data.
BEGIN;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS quarantine;
DO $$
BEGIN
    IF to_regclass('core.prices') IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='core' AND table_name='prices' AND column_name='price_sk'
    ) THEN
        RAISE EXCEPTION 'Legacy CORE schema detected. Explicit migration is required; bootstrap will not drop data.';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS audit.etl_batches (
    batch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
    status text NOT NULL DEFAULT 'running' CHECK(status IN ('running','success','failed')),
    error_message text
);
CREATE TABLE IF NOT EXISTS audit.etl_table_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES audit.etl_batches,
    source_table text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz, status text NOT NULL DEFAULT 'running',
    rows_read bigint NOT NULL DEFAULT 0, rows_inserted bigint NOT NULL DEFAULT 0,
    rows_skipped bigint NOT NULL DEFAULT 0, error_message text,
    UNIQUE(batch_id,source_table)
);
CREATE TABLE IF NOT EXISTS core.cities (
    gc_id integer PRIMARY KEY, canonical_source_id bigint NOT NULL,
    name text, full_name_gc text, full_name_local text,
    longitude double precision, latitude double precision
);
CREATE TABLE IF NOT EXISTS core.city_id_mapping (
    source_city_id bigint PRIMARY KEY,
    gc_id integer NOT NULL REFERENCES core.cities
);
CREATE TABLE IF NOT EXISTS core.body_types (
    std_id integer PRIMARY KEY, canonical_name text NOT NULL
);
CREATE TABLE IF NOT EXISTS core.body_type_id_mapping (
    source_body_type_id bigint PRIMARY KEY,
    std_id integer NOT NULL REFERENCES core.body_types,
    source_name text, id_from_1c integer
);
CREATE TABLE IF NOT EXISTS core.exchange_rates (
    rate_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id bigint NOT NULL, currency_code text NOT NULL, rate_date date NOT NULL,
    rate_value double precision NOT NULL CHECK(rate_value>0),
    record_hash text NOT NULL, batch_id uuid NOT NULL,
    observed_from timestamptz NOT NULL, observed_to timestamptz,
    is_current boolean NOT NULL DEFAULT true,
    CHECK(observed_to IS NULL OR observed_to>observed_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_exchange_rates_current
    ON core.exchange_rates(currency_code,rate_date) WHERE is_current;

CREATE TABLE IF NOT EXISTS core.prices (
    price_sk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id bigint NOT NULL,
    created_at timestamp, updated_at timestamp,
    subscriber_id integer, organization_id integer, loading_date date,
    loading_city_gcid integer REFERENCES core.cities,
    unloading_city_gcid integer REFERENCES core.cities,
    distance double precision, total_weight double precision,
    total_volume double precision,
    body_type integer REFERENCES core.body_type_id_mapping(source_body_type_id),
    price double precision, currency_code text, nearest_rate_date date,
    complex boolean, price_type smallint, item_id integer, is_fact boolean,
    meta_item_id bigint, end_loading_date date, source text,
    external_item_uuid uuid, outlier_mark boolean,
    weight_category text, volume_category text, price_group_id bigint,
    record_hash text NOT NULL, batch_id uuid NOT NULL,
    observed_from timestamptz NOT NULL, observed_to timestamptz,
    is_current boolean NOT NULL DEFAULT true,
    CHECK(observed_to IS NULL OR observed_to>observed_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_current
    ON core.prices(source_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_core_prices_loading_date ON core.prices(loading_date);
CREATE TABLE IF NOT EXISTS core.price_body_types (
    price_sk bigint NOT NULL REFERENCES core.prices,
    body_type_std_id integer NOT NULL REFERENCES core.body_types,
    PRIMARY KEY(price_sk,body_type_std_id)
);
CREATE TABLE IF NOT EXISTS core.price_dates (
    price_sk bigint NOT NULL REFERENCES core.prices, price_date date NOT NULL,
    PRIMARY KEY(price_sk,price_date)
);
CREATE TABLE IF NOT EXISTS core.price_participants (
    price_sk bigint NOT NULL REFERENCES core.prices, subscriber_id bigint NOT NULL,
    PRIMARY KEY(price_sk,subscriber_id)
);
CREATE TABLE IF NOT EXISTS core.price_group_stats (
    source_id bigint PRIMARY KEY, created_at timestamp, updated_at timestamp,
    loading_month date NOT NULL, loading_city_gcid integer, unloading_city_gcid integer,
    body_type_std_id integer, weight_category text, volume_category text,
    min_km_price double precision, max_km_price double precision,
    outliers_amount integer, clear_items_amount integer, avg_price double precision,
    record_hash text NOT NULL, batch_id uuid NOT NULL,
    observed_from timestamptz NOT NULL, observed_to timestamptz,
    is_current boolean NOT NULL DEFAULT true,
    CHECK(observed_to IS NULL OR observed_to>observed_from)
);
CREATE TABLE IF NOT EXISTS quarantine.invalid_records (
    quarantine_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table text NOT NULL, source_id bigint NOT NULL, source_batch_id uuid NOT NULL,
    record_hash text NOT NULL, record_data jsonb NOT NULL, error_codes text[] NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz,
    is_resolved boolean NOT NULL DEFAULT false,
    UNIQUE(source_table,source_id,record_hash)
);
COMMIT;
