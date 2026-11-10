"""Подготовка аналитического CORE из успешных RAW-батчей."""

import argparse

import json

import logging

import os

from uuid import UUID

import psycopg2

from psycopg2.extras import Json

LOG = logging.getLogger(__name__)

TABLES = (
    "cities",
    "body_types",
    "exchange_rates",
    "prices",
    "price_dates",
    "price_body_types",
    "price_participants",
    "price_group_stats",
    "favorite_directions",
    "selected_directions",
    "migrations",
)

PRICE_COLUMNS = """
created_at updated_at subscriber_id organization_id loading_date
loading_city_gcid unloading_city_gcid distance total_weight total_volume
body_type price currency_code nearest_rate_date complex price_type item_id
is_fact meta_item_id end_loading_date source external_item_uuid outlier_mark
weight_category volume_category price_group_id price_rub applied_rate
""".split()

PIPELINE_VERSION = "core_v2"

def connect():
    return psycopg2.connect(
        os.environ.get("DWH_DSN", ""),
        application_name="raw_to_core",
    )

def reject(cur, table, condition, code):
    # Имена таблиц и SQL-условия передаются только из этого модуля.
    cur.execute(
        f"""
        UPDATE s_{table} s
        SET errors = errors || %s::text[]
        WHERE {condition}
        """,
        ([code],),
    )

def snapshot(cur, batch_id):
    cur.execute("""
        SELECT t.source_table, t.rows_read
        FROM audit.etl_table_runs t
        JOIN audit.etl_batches b ON b.batch_id = t.batch_id
        WHERE t.batch_id = %s AND b.status = 'success'
          AND t.status = 'success' AND t.source_table = ANY(%s)
    """, (batch_id, list(TABLES)))

    expected = dict(cur.fetchall())
    if set(expected) != set(TABLES):
        raise RuntimeError(
            "Target RAW batch must have all 11 successful tasks"
        )

    # Исторические батчи проверяются отдельно для каждой таблицы.
    cur.execute("""
        CREATE TEMP TABLE eligible ON COMMIT DROP AS
        SELECT b.batch_id, b.started_at, t.source_table
        FROM audit.etl_batches b
        JOIN audit.etl_table_runs t ON t.batch_id = b.batch_id
        JOIN audit.etl_batches target ON target.batch_id = %s
        WHERE b.status = 'success' AND t.status = 'success'
          AND t.source_table = ANY(%s)
          AND (b.started_at, b.batch_id)
              <= (target.started_at, target.batch_id)
    """, (batch_id, list(TABLES)))

    cur.execute(
        "CREATE UNIQUE INDEX ON eligible(source_table, batch_id)"
    )

    for table in TABLES:
        cur.execute(f"""
            CREATE TABLE s_{table} AS
            SELECT r.*, to_jsonb(r) AS original,
                   ARRAY[]::text[] AS errors,
                   'accepted'::text AS outcome
            FROM (
                SELECT DISTINCT ON (r.id) r.*
                FROM raw.{table} r
                JOIN eligible b ON b.batch_id = r._batch_id
                WHERE b.source_table = %s
                ORDER BY r.id, b.started_at DESC, b.batch_id DESC
            ) r;

            CREATE UNIQUE INDEX ON s_{table}(id);
            ANALYZE s_{table};
        """, (table,))

        cur.execute(f"SELECT count(*) FROM s_{table}")
        actual = cur.fetchone()[0]

        if actual < expected[table]:
            raise RuntimeError(
                f"Incomplete RAW snapshot for {table}: "
                f"staged={actual}, "
                f"source_rows_read={expected[table]}. "
                "Check historical RAW data and task audit."
            )

        if table == "prices" and actual == 0:
            raise RuntimeError(
                "Empty prices snapshot; CORE update stopped"
            )

        LOG.info(
            "raw_snapshot table=%s staged=%s source_rows_read=%s",
            table, actual, expected[table],
        )

        reject(
            cur, table,
            "btrim(s._record_hash) = ''",
            "EMPTY_RECORD_HASH",
        )

    cur.execute("CREATE INDEX ON s_cities(gc_id)")
    cur.execute(
        "CREATE INDEX ON s_exchange_rates(num_code, setting_date)"
    )

def load_cities(cur):
    reject(cur, "cities", "btrim(s.name) = ''", "CITY_NAME_EMPTY")
    reject(
        cur,
        "cities",
        "btrim(s.full_name_local) = ''",
        "CITY_FULL_NAME_EMPTY",
    )
    reject(
        cur,
        "cities",
        """
        (
            s.latitude IS NOT NULL
            AND NOT (s.latitude BETWEEN -90 AND 90)
        )
        OR (
            s.longitude IS NOT NULL
            AND NOT (s.longitude BETWEEN -180 AND 180)
        )
        """,
        "INVALID_COORDINATES",
    )

    cur.execute(
        """
        UPDATE s_cities s
        SET outcome = 'deduplicated'
        WHERE errors = '{}'
          AND id <> (
              SELECT min(c.id)
              FROM s_cities c
              WHERE c.gc_id = s.gc_id
                AND c.errors = '{}'
          );

        INSERT INTO core.cities (
            gc_id, canonical_source_id, name,
            full_name_gc, full_name_local,
            longitude, latitude, region_name
        )
        SELECT
            gc_id,
            id,
            btrim(name),
            btrim(full_name_gc),
            btrim(full_name_local),
            longitude,
            latitude,
            CASE
                WHEN strpos(full_name_local, ',') > 0
                THEN nullif(
                    btrim(
                        substr(
                            full_name_local,
                            strpos(full_name_local, ',') + 1
                        )
                    ),
                    ''
                )
            END
        FROM s_cities
        WHERE errors = '{}'
          AND outcome = 'accepted'
        ON CONFLICT (gc_id) DO UPDATE SET
            canonical_source_id = EXCLUDED.canonical_source_id,
            name = EXCLUDED.name,
            full_name_gc = EXCLUDED.full_name_gc,
            full_name_local = EXCLUDED.full_name_local,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            region_name = EXCLUDED.region_name;

        INSERT INTO core.city_id_mapping(source_city_id, gc_id)
        SELECT id, gc_id
        FROM s_cities
        WHERE errors = '{}'
        ON CONFLICT (source_city_id) DO UPDATE SET
            gc_id = EXCLUDED.gc_id;
        """
    )


def load_body_types(cur):
    reject(
        cur,
        "body_types",
        "btrim(s.name) = ''",
        "BODY_NAME_EMPTY",
    )

    cur.execute(
        """
        UPDATE s_body_types
        SET std_id = CASE
            WHEN id IN (56, 57, 58, 59, 60, 61) THEN 56
            WHEN id IN (4, 46, 47, 48) THEN 4
            WHEN id = 2 THEN 2
            WHEN id IN (1, 15) THEN 15
            WHEN std_id IN (56, 4, 2, 15) THEN std_id
            ELSE 0
        END;

        INSERT INTO core.body_types(std_id, canonical_name)
        VALUES
            (-1, 'Не указан'),
            (56, 'Тент'),
            (4, 'Рефрижератор'),
            (2, 'Изотерм'),
            (15, 'Борт'),
            (0, 'Прочие')
        ON CONFLICT (std_id) DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name;

        INSERT INTO core.body_type_id_mapping (
            source_body_type_id, std_id,
            source_name, id_from_1c
        )
        SELECT id, std_id, name, "id_from_1C"
        FROM s_body_types
        WHERE errors = '{}'
        ON CONFLICT (source_body_type_id) DO UPDATE SET
            std_id = EXCLUDED.std_id,
            source_name = EXCLUDED.source_name,
            id_from_1c = EXCLUDED.id_from_1c;
        """
    )


def sync_history(
    cur, table, select_sql, columns, key, batch_id, observed_at
):
    cur.execute(
        f"""
        CREATE TEMP TABLE accepted_{table}
        ON COMMIT DROP AS {select_sql}
        """
    )
    cur.execute(
        f"""
        CREATE UNIQUE INDEX
        ON accepted_{table} ({", ".join(key)})
        """
    )
    cur.execute(f"ANALYZE accepted_{table}")

    match = " AND ".join(f"a.{c} = c.{c}" for c in key)
    equal = " AND ".join(
        f"a.{c} IS NOT DISTINCT FROM c.{c}"
        for c in columns
    )

    cur.execute(
        f"""
        UPDATE core.{table} c
        SET is_current = false,
            observed_to = %s
        WHERE c.is_current
          AND NOT EXISTS (
              SELECT 1
              FROM accepted_{table} a
              WHERE {match} AND {equal}
          )
        """,
        (observed_at,),
    )
    closed = cur.rowcount

    cur.execute(
        f"""
        INSERT INTO core.{table} (
            {", ".join(columns)},
            batch_id, observed_from, is_current
        )
        SELECT
            {", ".join("a." + c for c in columns)},
            %s, %s, true
        FROM accepted_{table} a
        WHERE NOT EXISTS (
            SELECT 1
            FROM core.{table} c
            WHERE c.is_current AND {match}
        )
        """,
        (batch_id, observed_at),
    )

    return {
        "inserted_versions": cur.rowcount,
        "closed_versions": closed,
    }

def rates(cur, batch_id, observed_at):
    reject(
        cur,
        "exchange_rates",
        "s.num_code NOT IN ('643', '840', '978')",
        "UNSUPPORTED_CURRENCY",
    )
    reject(
        cur,
        "exchange_rates",
        "NOT (s.value > 0 AND s.value < 'Infinity'::float8)",
        "INVALID_RATE",
    )
    reject(
        cur,
        "exchange_rates",
        "s.num_code = '643' AND s.value <> 1",
        "RUB_RATE_NOT_ONE",
    )
    reject(
        cur,
        "exchange_rates",
        """
        EXISTS (
            SELECT 1
            FROM s_exchange_rates r
            WHERE r.num_code = s.num_code
              AND r.setting_date = s.setting_date
            GROUP BY r.num_code, r.setting_date
            HAVING count(DISTINCT r.value) > 1
        )
        """,
        "CONFLICTING_RATE_VALUES",
    )

    cur.execute(
        """
        UPDATE s_exchange_rates s
        SET outcome = 'deduplicated'
        WHERE errors = '{}'
          AND id <> (
              SELECT min(r.id)
              FROM s_exchange_rates r
              WHERE r.num_code = s.num_code
                AND r.setting_date = s.setting_date
                AND r.errors = '{}'
          )
        """
    )

    return sync_history(
        cur,
        "exchange_rates",
        """
        SELECT
            id AS source_id,
            num_code AS currency_code,
            setting_date AS rate_date,
            value AS rate_value,
            _record_hash AS record_hash
        FROM s_exchange_rates
        WHERE errors = '{}'
          AND outcome = 'accepted'
        """,
        [
            "source_id", "currency_code", "rate_date",
            "rate_value", "record_hash",
        ],
        ["currency_code", "rate_date"],
        batch_id,
        observed_at,
    )

def check_references(
    cur,
    table,
    loading="loading_city_gcid",
    unloading="unloading_city_gcid",
):
    for field in (loading, unloading):
        reject(
            cur,
            table,
            f"""
            NOT EXISTS (
                SELECT 1
                FROM s_cities c
                WHERE c.gc_id = s.{field}
                  AND c.errors = '{{}}'
            )
            """,
            field.upper() + "_NOT_FOUND",
        )

def prices(cur, batch_id, observed_at):
    check_references(cur, "prices")

    for field, positive in (
        ("price", True),
        ("distance", True),
        ("total_weight", False),
        ("total_volume", False),
    ):
        operator = ">" if positive else ">="
        reject(
            cur,
            "prices",
            f"""
            s.{field} IS NULL
            OR NOT (
                s.{field} {operator} 0
                AND s.{field} < 'Infinity'::float8
            )
            """,
            "INVALID_" + field.upper(),
        )

    reject(
        cur, "prices",
        "s.currency_code = '643' AND s.price <= 1000",
        "RUB_PRICE_TOO_LOW",
    )
    reject(
        cur, "prices",
        "s.currency_code NOT IN ('643', '840', '978')",
        "UNSUPPORTED_CURRENCY",
    )
    reject(
        cur,
        "prices",
        """
        s.body_type IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM s_body_types b
            WHERE b.id = s.body_type AND b.errors = '{}'
        )
        """,
        "BODY_TYPE_NOT_FOUND",
    )
    reject(
        cur,
        "prices",
        """
        s.currency_code IN ('840', '978')
        AND NOT EXISTS (
            SELECT 1
            FROM core.exchange_rates r
            WHERE r.is_current
              AND r.currency_code = s.currency_code
              AND r.rate_date = s.nearest_rate_date
        )
        """,
        "EXACT_RATE_NOT_FOUND",
    )
    reject(
        cur,
        "prices",
        """
        s.price_type NOT IN (1, 2, 3, 4, 5, 6, 9)
        OR s.is_fact IS DISTINCT FROM (s.price_type <> 6)
        """,
        "INVALID_PRICE_TYPE_OR_FACT",
    )
    reject(
        cur, "prices",
        "s.end_loading_date < s.loading_date",
        "INVALID_DATE_RANGE",
    )

    cur.execute(
        """
        ALTER TABLE s_prices ADD COLUMN price_rub numeric;
        ALTER TABLE s_prices ADD COLUMN applied_rate numeric;

        UPDATE s_prices s
        SET applied_rate = fx.rate,
            price_rub = round(s.price::numeric * fx.rate, 2),
            weight_category = CASE
                WHEN nullif(btrim(s.weight_category), '') IS NOT NULL
                    THEN upper(btrim(s.weight_category))
                WHEN round(s.total_weight::numeric, 1) >= 30 THEN 'G20T'
                WHEN round(s.total_weight::numeric, 1) >= 10 THEN 'FTL'
                WHEN round(s.total_weight::numeric, 1) >= 5 THEN '10T'
                WHEN round(s.total_weight::numeric, 1) >= 2.5 THEN '5T'
                WHEN round(s.total_weight::numeric, 1) >= 0.5 THEN 'L5T'
                ELSE 'OTH'
            END,
            volume_category = coalesce(
                nullif(upper(btrim(s.volume_category)), ''),
                CASE
                    WHEN s.total_volume > 0 AND s.total_volume <= 120
                        THEN 'FTL'
                    ELSE 'OTH'
                END
            )
        FROM (
            SELECT
                p.id,
                CASE
                    WHEN p.currency_code = '643' THEN 1
                    ELSE r.rate_value::numeric
                END AS rate
            FROM s_prices p
            LEFT JOIN core.exchange_rates r
                ON r.currency_code = p.currency_code
               AND r.rate_date = p.nearest_rate_date
               AND r.is_current
        ) fx
        WHERE fx.id = s.id AND s.errors = '{}';
        """
    )

    reject(
        cur,
        "prices",
        """
        s.weight_category IS NOT NULL
        AND upper(btrim(s.weight_category))
            NOT IN ('', 'G20T', 'FTL', '10T', '5T', 'L5T', 'OTH')
        """,
        "INVALID_WEIGHT_CATEGORY",
    )
    reject(
        cur,
        "prices",
        """
        s.volume_category IS NOT NULL
        AND upper(btrim(s.volume_category)) NOT IN ('', 'FTL', 'OTH')
        """,
        "INVALID_VOLUME_CATEGORY",
    )
    reject(
        cur,
        "prices",
        """
        s.errors = '{}'
        AND (s.price_rub IS NULL OR s.price_rub <= 0)
        """,
        "INVALID_PRICE_RUB",
    )

    return sync_history(
        cur,
        "prices",
        f"""
        SELECT
            id AS source_id,
            {", ".join(PRICE_COLUMNS)},
            _record_hash AS record_hash
        FROM s_prices
        WHERE errors = '{{}}'
        """,
        ["source_id"] + PRICE_COLUMNS + ["record_hash"],
        ["source_id"],
        batch_id,
        observed_at,
    )

def children(cur, selected):
    definitions = {
        "price_dates": ("s.price_date", "price_date"),
        "price_body_types": ("b.std_id", "body_type_std_id"),
        "price_participants": ("s.subscriber_id", "subscriber_id"),
    }
    for table, (expression, column) in definitions.items():
        if table != selected:
            continue
        reject(
            cur,
            table,
            """
            NOT EXISTS (
                SELECT 1
                FROM s_prices p
                WHERE p.id = s.price_id AND p.errors = '{}'
            )
            """,
            "PARENT_PRICE_REJECTED_OR_MISSING",
        )

        body_join = ""

        if table == "price_body_types":
            reject(
                cur,
                table,
                """
                NOT EXISTS (
                    SELECT 1
                    FROM s_body_types b
                    WHERE b.id = s.body_type_id AND b.errors = '{}'
                )
                """,
                "BODY_TYPE_NOT_FOUND",
            )
            body_join = "JOIN s_body_types b ON b.id = s.body_type_id"

        cur.execute(
            f"""
            CREATE TEMP TABLE child_{table} ON COMMIT DROP AS
            SELECT
                s.id,
                p.price_sk,
                {expression} AS value,
                row_number() OVER (
                    PARTITION BY p.price_sk, {expression}
                    ORDER BY s.id
                ) AS rn
            FROM s_{table} s
            JOIN core.prices p
                ON p.source_id = s.price_id AND p.is_current
            {body_join}
            WHERE s.errors = '{{}}';

            UPDATE s_{table} s
            SET outcome = 'deduplicated'
            FROM child_{table} c
            WHERE c.id = s.id AND c.rn > 1;

            DELETE FROM core.{table} c
            USING core.prices p
            WHERE c.price_sk = p.price_sk AND p.is_current;

            INSERT INTO core.{table}(price_sk, {column})
            SELECT price_sk, value
            FROM child_{table}
            WHERE rn = 1;
            """
        )

    if selected != "price_body_types":
        return

    cur.execute(
        """
        INSERT INTO core.price_body_types (
            price_sk, body_type_std_id, source_kind
        )
        SELECT
            p.price_sk,
            coalesce(m.std_id, -1),
            CASE
                WHEN m.std_id IS NULL THEN 'missing'
                ELSE 'primary'
            END
        FROM core.prices p
        LEFT JOIN core.body_type_id_mapping m
            ON m.source_body_type_id = p.body_type
        WHERE p.is_current
          AND NOT EXISTS (
              SELECT 1
              FROM core.price_body_types b
              WHERE b.price_sk = p.price_sk
          );
        """
    )


def validate_directions(cur, selected):
    for table in ("favorite_directions", "selected_directions"):
        if table != selected:
            continue
        check_references(
            cur, table,
            "loading_city_gc_id", "unloading_city_gc_id",
        )
        reject(
            cur,
            table,
            """
            NOT EXISTS (
                SELECT 1
                FROM s_body_types b
                WHERE b.id = s.body_type_id AND b.errors = '{}'
            )
            """,
            "BODY_TYPE_NOT_FOUND",
        )
        cur.execute(
            f"""
            UPDATE s_{table} s
            SET outcome = 'deduplicated'
            WHERE errors = '{{}}'
              AND EXISTS (
                  SELECT 1
                  FROM s_{table} d
                  JOIN s_body_types db ON db.id = d.body_type_id
                  JOIN s_body_types sb ON sb.id = s.body_type_id
                  WHERE d.errors = '{{}}'
                    AND d.id < s.id
                    AND d.subscriber_id = s.subscriber_id
                    AND d.loading_city_gc_id = s.loading_city_gc_id
                    AND d.unloading_city_gc_id = s.unloading_city_gc_id
                    AND db.std_id = sb.std_id
              )
            """
        )


def load_directions(cur, batch_id):
    cur.execute(
        """
        DELETE FROM core.subscriber_directions;

        INSERT INTO core.subscriber_directions (
            subscriber_id, loading_city_gcid, unloading_city_gcid,
            body_type_std_id, is_favorite, is_selected, batch_id
        )
        SELECT
            d.subscriber_id,
            d.loading_city_gc_id,
            d.unloading_city_gc_id,
            b.std_id,
            bool_or(d.favorite),
            bool_or(d.selected),
            %s
        FROM (
            SELECT
                subscriber_id, loading_city_gc_id,
                unloading_city_gc_id, body_type_id,
                true AS favorite, false AS selected
            FROM s_favorite_directions
            WHERE errors = '{}'

            UNION ALL

            SELECT
                subscriber_id, loading_city_gc_id,
                unloading_city_gc_id, body_type_id,
                false, true
            FROM s_selected_directions
            WHERE errors = '{}'
        ) d
        JOIN s_body_types b ON b.id = d.body_type_id
        GROUP BY
            d.subscriber_id, d.loading_city_gc_id,
            d.unloading_city_gc_id, b.std_id
        """,
        (batch_id,),
    )


def validate_migrations(cur):
    reject(
        cur, "migrations",
        "btrim(s.migration) = ''",
        "MIGRATION_NAME_EMPTY",
    )
    cur.execute("UPDATE s_migrations SET outcome = 'validated_only'")


def load_group_stats(cur, batch_id, observed_at):
    check_references(cur, "price_group_stats")
    reject(
        cur,
        "price_group_stats",
        """
        NOT EXISTS (
            SELECT 1
            FROM s_body_types b
            WHERE (
                b.std_id = s.body_type_std_id
                OR (b.original->>'std_id')::integer = s.body_type_std_id
            )
            AND b.errors = '{}'
        )
        """,
        "BODY_GROUP_NOT_FOUND",
    )
    cur.execute(
        """
        UPDATE s_price_group_stats
        SET body_type_std_id = 0
        WHERE body_type_std_id NOT IN (56, 4, 2, 15)
          AND errors = '{}'
        """
    )

    for field in ("avg_price", "min_km_price", "max_km_price"):
        reject(
            cur,
            "price_group_stats",
            f"""
            s.{field} IS NOT NULL
            AND NOT (
                s.{field} >= 0
                AND s.{field} < 'Infinity'::float8
            )
            """,
            "INVALID_" + field.upper(),
        )

    reject(
        cur, "price_group_stats",
        "s.min_km_price > s.max_km_price",
        "INVALID_PRICE_RANGE",
    )

    columns = """
    created_at updated_at loading_month loading_city_gcid
    unloading_city_gcid body_type_std_id weight_category
    volume_category min_km_price max_km_price outliers_amount
    clear_items_amount avg_price
    """.split()

    # Здесь в исходном DDL PK(source_id), поэтому сохраняем SCD1.
    cur.execute(
        """
        DELETE FROM core.price_group_stats c
        WHERE NOT EXISTS (
            SELECT 1
            FROM s_price_group_stats s
            WHERE s.id = c.source_id AND s.errors = '{}'
        )
        """
    )
    cur.execute(
        f"""
        INSERT INTO core.price_group_stats (
            source_id, {", ".join(columns)},
            record_hash, batch_id, observed_from
        )
        SELECT
            id, {", ".join(columns)}, _record_hash, %s, %s
        FROM s_price_group_stats
        WHERE errors = '{{}}'
        ON CONFLICT (source_id) DO UPDATE SET
            {", ".join(f"{c} = EXCLUDED.{c}" for c in columns)},
            record_hash = EXCLUDED.record_hash,
            batch_id = EXCLUDED.batch_id,
            observed_from = EXCLUDED.observed_from,
            observed_to = NULL,
            is_current = true
        WHERE ROW(
            {", ".join("core.price_group_stats." + c for c in columns)},
            core.price_group_stats.record_hash
        ) IS DISTINCT FROM ROW(
            {", ".join("EXCLUDED." + c for c in columns)},
            EXCLUDED.record_hash
        )
        """,
        (batch_id, observed_at),
    )


def save_results(cur, batch_id, selected):
    summary = {}

    for table in (selected,):
        cur.execute(
            f"""
            UPDATE s_{table}
            SET outcome = 'rejected'
            WHERE errors <> '{{}}'
            """
        )
        cur.execute(
            f"""
            INSERT INTO quarantine.invalid_records (
                source_table, source_id, source_batch_id,
                record_hash, record_data, error_codes
            )
            SELECT
                %s, id, _batch_id, _record_hash, original, errors
            FROM s_{table}
            WHERE outcome = 'rejected'
            ON CONFLICT (
                source_table, source_id, record_hash
            ) DO UPDATE SET
                source_batch_id = EXCLUDED.source_batch_id,
                record_data = EXCLUDED.record_data,
                error_codes = EXCLUDED.error_codes,
                is_resolved = false,
                resolved_at = NULL;

            UPDATE quarantine.invalid_records q
            SET is_resolved = true,
                resolved_at = clock_timestamp()
            FROM s_{table} s
            WHERE q.source_table = %s
              AND q.source_id = s.id
              AND NOT q.is_resolved
              AND s.errors = '{{}}';

            INSERT INTO audit.raw_to_core_results (
                run_batch_id, source_table, source_id,
                source_batch_id, record_hash, outcome, error_codes
            )
            SELECT
                %s, %s, id, _batch_id,
                _record_hash, outcome, errors
            FROM s_{table};

            SELECT outcome, count(*)
            FROM s_{table}
            GROUP BY outcome;
            """,
            (table, table, batch_id, table),
        )

        counts = dict(cur.fetchall())
        summary[table] = {"read": sum(counts.values()), **counts}

    return summary


def run(raw_batch_id):
    from .raw_to_core_tasks import run as run_tasks
    return run_tasks(raw_batch_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-batch-id", required=True)
    print(
        json.dumps(
            run(parser.parse_args().raw_batch_id),
            ensure_ascii=False,
            indent=2,
        )
    )

