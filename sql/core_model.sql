CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS core.cities (
    gc_id BIGINT PRIMARY KEY,
    canonical_source_id BIGINT NOT NULL,
    name TEXT,
    full_name_gc TEXT,
    full_name_local TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS core.city_id_mapping (
    source_city_id BIGINT PRIMARY KEY,
    gc_id BIGINT NOT NULL REFERENCES core.cities(gc_id)
);

CREATE TABLE IF NOT EXISTS core.body_types (
    std_id INTEGER PRIMARY KEY,
    canonical_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.body_type_id_mapping (
    source_body_type_id BIGINT PRIMARY KEY,
    std_id INTEGER NOT NULL REFERENCES core.body_types(std_id),
    source_name TEXT,
    id_from_1c INTEGER
);

CREATE TABLE IF NOT EXISTS core.exchange_rates (
    source_id BIGINT PRIMARY KEY,
    num_code INTEGER NOT NULL,
    rate_value NUMERIC(20, 8) NOT NULL,
    setting_date DATE NOT NULL
);

-- Одна строка на одну исходную цену. Дочерние сущности собраны в JSON-массивы,
-- чтобы соединения не размножали факт.
CREATE TABLE IF NOT EXISTS core.prices (
    price_id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    subscriber_id BIGINT,
    organization_id BIGINT,
    loading_date DATE,
    loading_city_gcid BIGINT,
    unloading_city_gcid BIGINT,
    distance NUMERIC(18, 4),
    total_weight NUMERIC(18, 4),
    total_volume NUMERIC(18, 4),
    body_type TEXT,
    price NUMERIC(20, 4),
    currency_code INTEGER,
    complex BOOLEAN,
    price_type SMALLINT,
    item_id INTEGER,
    is_fact BOOLEAN,
    nearest_rate_date DATE,
    meta_item_id BIGINT,
    end_loading_date DATE,
    source TEXT,
    external_item_uuid UUID,
    outlier_mark BOOLEAN,
    weight_category TEXT,
    volume_category TEXT,
    price_group_id BIGINT,
    price_dates JSONB NOT NULL DEFAULT '[]'::jsonb,
    body_type_std_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    participant_ids JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS core.price_group_stats (
    source_id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loading_month DATE,
    loading_city_gcid BIGINT,
    unloading_city_gcid BIGINT,
    body_type_std_id INTEGER,
    weight_category TEXT,
    volume_category TEXT,
    min_km_price NUMERIC(20, 4),
    max_km_price NUMERIC(20, 4),
    outliers_amount BIGINT,
    clear_items_amount BIGINT,
    avg_price NUMERIC(20, 4)
);

-- Аудит создаётся только если таблиц ещё нет.
CREATE TABLE IF NOT EXISTS audit.etl_batches (
    batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running','success','failed')),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.etl_table_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES audit.etl_batches(batch_id),
    source_table TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_skipped BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    UNIQUE(batch_id, source_table)
);

CREATE INDEX IF NOT EXISTS idx_core_prices_loading_date ON core.prices(loading_date);
CREATE INDEX IF NOT EXISTS idx_core_prices_route ON core.prices(loading_city_gcid, unloading_city_gcid);
CREATE INDEX IF NOT EXISTS idx_core_group_stats_month ON core.price_group_stats(loading_month);
