"""Полная атомарная пересборка рыночной витрины из подготовленного CORE."""

import base64
import csv
import gzip
import hashlib
import io
import json
import logging
import os
from datetime import date
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg2


LOG = logging.getLogger(__name__)

DB = "analytics"
TARGET = "mart_market_corridor"

KEY = (
    "month, loading_city_gcid, unloading_city_gcid, "
    "body_group_id, weight_category"
)

FACT = """
    price_id Int64,
    month Date,
    loading_city_gcid Int32,
    unloading_city_gcid Int32,
    body_group_id Int32,
    weight_category LowCardinality(String),
    loading_city String,
    unloading_city String,
    body_group String,
    price_rub Decimal(20,2),
    distance Float64,
    carrier_id Nullable(Int64),
    source_price_date Date
"""

MART = """
    month Date,
    loading_city_gcid Int32,
    unloading_city_gcid Int32,
    body_group_id Int32,
    weight_category LowCardinality(String),
    loading_city String,
    unloading_city String,
    body_group String,
    p25_trip Float64,
    median_trip Float64,
    p75_trip Float64,
    avg_trip Float64,
    p25_km Float64,
    median_km Float64,
    p75_km Float64,
    trips_count UInt64,
    carriers_count UInt64,
    cv_rate Nullable(Float64),
    last_price_date Date,
    mom_pct Nullable(Float64),
    yoy_pct Nullable(Float64),
    reliability_level LowCardinality(String),
    as_of Date
"""

EXPORT = """
WITH months AS (
    SELECT
        price_sk,
        date_trunc('month', price_date)::date AS month,
        max(price_date) AS source_price_date
    FROM core.price_dates
    WHERE price_date <= %s
    GROUP BY price_sk, date_trunc('month', price_date)::date
)
SELECT
    p.source_id,
    d.month,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    b.body_type_std_id,
    p.weight_category,
    coalesce(lc.name, 'Не указан'),
    coalesce(uc.name, 'Не указан'),
    bt.canonical_name,
    p.price_rub,
    p.distance,
    CASE
        WHEN p.price_type IN (3, 9) THEN p.organization_id
    END,
    d.source_price_date
FROM core.prices p
JOIN months d
    ON d.price_sk = p.price_sk
JOIN core.price_body_types b
    ON b.price_sk = p.price_sk
JOIN core.cities lc
    ON lc.gc_id = p.loading_city_gcid
JOIN core.cities uc
    ON uc.gc_id = p.unloading_city_gcid
JOIN core.body_types bt
    ON bt.std_id = b.body_type_std_id
WHERE p.is_current
  AND p.is_fact
  AND p.price_type IN (1, 2, 3, 4, 5, 9)
  AND p.complex = false
  AND p.outlier_mark = false
"""


def ch(sql, payload=None):
    """Запрос ClickHouse через HTTP, без дополнительного драйвера."""
    credentials = (
        os.getenv("CH_USER", "default")
        + ":"
        + os.getenv("CH_PASSWORD", "")
    )

    headers = {
        "Authorization": (
            "Basic "
            + base64.b64encode(credentials.encode()).decode()
        )
    }
    params = {
        "wait_end_of_query": 1,
        "max_threads": 2,
    }

    if payload is None:
        data = sql.encode()
    else:
        params["query"] = sql
        data = gzip.compress(payload, compresslevel=1)
        headers["Content-Encoding"] = "gzip"

    url = (
        os.getenv("CH_URL", "http://clickhouse:8123").rstrip("/")
        + "/?"
        + urlencode(params)
    )

    request = Request(url, data=data, headers=headers)

    try:
        with urlopen(request, timeout=600) as response:
            result = response.read().decode()
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:8000]
        raise RuntimeError(
            f"ClickHouse HTTP {exc.code}: {detail}"
        ) from None

    if result.startswith("Code:"):
        raise RuntimeError(result)

    return result


def rows(sql):
    return json.loads(ch(sql + " FORMAT JSON"))["data"]


def names(run_id):
    tag = hashlib.sha256(run_id.encode()).hexdigest()[:24]
    tables = [
        f"{DB}._mart_{tag}_{part}"
        for part in ("facts", "agg", "ready")
    ]
    return tag, tables


def prepare(run_id, as_of="2026-10-26"):
    """Создание публичной витрины и staging конкретного запуска."""
    as_of = date.fromisoformat(as_of).isoformat()
    tag, tables = names(run_id)

    ch(f"CREATE DATABASE IF NOT EXISTS {DB} ENGINE=Atomic")

    engine = rows(
        f"SELECT engine FROM system.databases WHERE name='{DB}'"
    )[0]["engine"]

    if engine != "Atomic":
        raise RuntimeError(
            "analytics must use Atomic engine for EXCHANGE TABLES"
        )

    definitions = [
        (f"{DB}.{TARGET}", MART, KEY),
        (tables[0], FACT, KEY + ", price_id"),
        (tables[1], MART, KEY),
        (tables[2], MART, KEY),
    ]

    for table, ddl, order in definitions:
        ch(
            f"""
            CREATE TABLE IF NOT EXISTS {table} ({ddl})
            ENGINE=MergeTree
            PARTITION BY toYYYYMM(month)
            ORDER BY ({order})
            """
        )

    expected = [
        (r["name"], r["type"])
        for r in rows(f"DESCRIBE TABLE {tables[2]}")
    ]
    actual = [
        (r["name"], r["type"])
        for r in rows(f"DESCRIBE TABLE {DB}.{TARGET}")
    ]

    if actual != expected:
        raise RuntimeError(
            "Existing MART schema differs; explicit migration is required"
        )

    return {
        "tag": tag,
        "tables": tables,
        "as_of": as_of,
    }


def load(ctx):
    """Согласованный снимок CORE → ClickHouse пакетами по 25 000."""
    table = ctx["tables"][0]

    # Повторная попытка перечитывает снимок целиком.
    ch(f"TRUNCATE TABLE {table}")

    # Использует тот же DWH_DSN или PG* variables, что и CORE.
    conn = psycopg2.connect(
        os.getenv("DWH_DSN", ""),
        application_name="core_to_mart",
        connect_timeout=10,
    )
    conn.set_session(isolation_level="REPEATABLE READ")

    count = 0

    try:
        with conn:
            with conn.cursor() as cur:
                # Совместимо с протоколом блокировок текущего CORE.
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(73021, 1)"
                )
                if not cur.fetchone()[0]:
                    raise RuntimeError(
                        "CORE is being updated; retry MART after completion"
                    )

                cur.execute(
                    "SELECT count(*) FROM core_stage.active_run"
                )
                if cur.fetchone()[0]:
                    raise RuntimeError(
                        "CORE has an unfinished batch; "
                        "complete it before MART"
                    )

                cur.execute(
                    """
                    SELECT count(*)
                    FROM audit.raw_to_core_runs
                    WHERE status = 'success'
                    """
                )
                if not cur.fetchone()[0]:
                    raise RuntimeError("No successful CORE batch")

                cur.execute(
                    """
                    SELECT count(*)
                    FROM core.prices p
                    WHERE is_current
                      AND (
                        is_fact IS NULL
                        OR price_type IS NULL
                        OR complex IS NULL
                        OR outlier_mark IS NULL
                        OR price_rub IS NULL
                        OR price_rub <= 0
                        OR price_rub >= 'Infinity'::numeric
                        OR distance IS NULL
                        OR NOT (
                            distance > 0
                            AND distance < 'Infinity'::float8
                        )
                        OR weight_category IS NULL
                        OR NOT EXISTS (
                            SELECT 1
                            FROM core.price_body_types b
                            WHERE b.price_sk = p.price_sk
                        )
                      )
                    """
                )
                if cur.fetchone()[0]:
                    raise RuntimeError(
                        "Invalid current CORE prices: "
                        "run CORE checks first"
                    )

                # Причины исключения взаимно исключающие.
                cur.execute(
                    """
                    SELECT
                        count(*) AS current_prices,

                        count(*) FILTER (
                            WHERE NOT is_fact
                               OR price_type NOT IN (1,2,3,4,5,9)
                        ) AS non_fact,

                        count(*) FILTER (
                            WHERE is_fact
                              AND price_type IN (1,2,3,4,5,9)
                              AND complex
                        ) AS complex,

                        count(*) FILTER (
                            WHERE is_fact
                              AND price_type IN (1,2,3,4,5,9)
                              AND NOT complex
                              AND outlier_mark
                        ) AS outliers,

                        count(*) FILTER (
                            WHERE is_fact
                              AND price_type IN (1,2,3,4,5,9)
                              AND NOT complex
                              AND NOT outlier_mark
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM core.price_dates d
                                  WHERE d.price_sk = p.price_sk
                                    AND d.price_date <= %s
                              )
                        ) AS no_historical_dates

                    FROM core.prices p
                    WHERE is_current
                    """,
                    (ctx["as_of"],),
                )

                report = dict(
                    zip(
                        [column.name for column in cur.description],
                        cur.fetchone(),
                    )
                )

            if report["current_prices"] == 0:
                raise RuntimeError(
                    "CORE has no current prices; "
                    "MART publication stopped"
                )

            with conn.cursor(name="mart_export") as cur:
                cur.itersize = 25000
                cur.execute(EXPORT, (ctx["as_of"],))

                while True:
                    batch = cur.fetchmany(25000)
                    if not batch:
                        break

                    buffer = io.StringIO()
                    writer = csv.writer(
                        buffer,
                        lineterminator="\n",
                    )
                    writer.writerows(
                        tuple(
                            "\\N" if value is None else value
                            for value in row
                        )
                        for row in batch
                    )

                    ch(
                        f"INSERT INTO {table} FORMAT CSV",
                        buffer.getvalue().encode(),
                    )
                    count += len(batch)

        report["fact_rows"] = count
        LOG.info("mart_export %s", json.dumps(report))
        return report

    finally:
        conn.close()


def aggregate(ctx):
    """Точные квартили, число рейсов/перевозчиков и надёжность."""
    fact, agg, _ = ctx["tables"]

    ch(f"TRUNCATE TABLE {agg}")

    ch(
        f"""
        INSERT INTO {agg}
        SELECT
            {KEY},
            any(loading_city),
            any(unloading_city),
            any(body_group),

            (
                quantilesExactInclusive(0.25,0.5,0.75)(
                    toFloat64(price_rub)
                ) AS q
            )[1],
            q[2],
            q[3],
            avg(toFloat64(price_rub)),

            (
                quantilesExactInclusive(0.25,0.5,0.75)(
                    toFloat64(price_rub) / distance
                ) AS k
            )[1],
            k[2],
            k[3],

            count(),
            uniqExact(carrier_id),

            if(
                count() < 2,
                NULL,
                stddevSamp(toFloat64(price_rub))
                    / avg(toFloat64(price_rub))
            ),

            max(source_price_date),
            NULL,
            NULL,

            multiIf(
                count() >= 30
                AND uniqExact(carrier_id) >= 5
                AND dateDiff(
                    'day',
                    max(source_price_date),
                    toDate('{ctx["as_of"]}')
                ) <= 30,
                'HIGH',

                count() >= 10,
                'MEDIUM',

                'LOW_DATA'
            ),

            toDate('{ctx["as_of"]}')

        FROM {fact}
        GROUP BY {KEY}
        SETTINGS max_threads=2
        """
    )


def compare(ctx):
    """Сравнение с календарным предыдущим месяцем и прошлым годом."""
    _, agg, ready = ctx["tables"]

    ch(f"TRUNCATE TABLE {ready}")

    joins = " AND ".join(
        f"a.{key}=p.{key}"
        for key in KEY.split(", ")[1:]
    )
    joins_y = joins.replace("p.", "y.")

    ch(
        f"""
        INSERT INTO {ready}
        SELECT
            a.* EXCEPT(
                mom_pct,
                yoy_pct,
                reliability_level,
                as_of
            ),

            100 * (
                a.median_trip / nullIf(p.median_trip, 0) - 1
            ),

            100 * (
                a.median_trip / nullIf(y.median_trip, 0) - 1
            ),

            a.reliability_level,
            a.as_of

        FROM {agg} a

        LEFT JOIN {agg} p
            ON p.month = addMonths(a.month, -1)
           AND {joins}

        LEFT JOIN {agg} y
            ON y.month = addMonths(a.month, -12)
           AND {joins_y}

        SETTINGS join_use_nulls=1
        """
    )


def check_facts(ctx, report):
    """Проверка полноты выгрузки, дублей и будущих дат."""
    fact = ctx["tables"][0]

    result = rows(
        f"""
        SELECT
            count() AS n,
            uniqExact(price_id) AS price_count,
            uniqExact(tuple(price_id, {KEY})) AS unique_n,

            countIf(
                price_rub <= 0
                OR distance <= 0
                OR NOT isFinite(distance)
                OR source_price_date > toDate('{ctx["as_of"]}')
            ) AS bad

        FROM {fact}
        """
    )[0]

    expected_prices = report["current_prices"] - sum(
        report[key]
        for key in (
            "non_fact",
            "complex",
            "outliers",
            "no_historical_dates",
        )
    )

    invalid = (
        int(result["price_count"]) != expected_prices
        or int(result["n"]) != report["fact_rows"]
        or result["n"] != result["unique_n"]
        or int(result["bad"]) != 0
    )

    if invalid:
        raise RuntimeError(f"Invalid MART facts: {result}")

    return result


def check_mart(ctx):
    """Проверка grain, итоговых счётчиков и метрик."""
    fact, agg, ready = ctx["tables"]

    result = rows(
        f"""
        SELECT
            count() AS n,
            uniqExact(tuple({KEY})) AS unique_n,
            sum(trips_count) AS trips,

            countIf(
                NOT isFinite(p25_trip)
                OR NOT isFinite(median_trip)
                OR NOT isFinite(p75_trip)
                OR NOT isFinite(avg_trip)
                OR NOT isFinite(p25_km)
                OR NOT isFinite(median_km)
                OR NOT isFinite(p75_km)
                OR (
                    cv_rate IS NOT NULL
                    AND NOT isFinite(cv_rate)
                )
                OR p25_trip > median_trip
                OR median_trip > p75_trip
                OR p25_km > median_km
                OR median_km > p75_km
                OR carriers_count > trips_count
                OR (
                    mom_pct IS NOT NULL
                    AND NOT isFinite(mom_pct)
                )
                OR (
                    yoy_pct IS NOT NULL
                    AND NOT isFinite(yoy_pct)
                )
            ) AS bad

        FROM {ready}
        """
    )[0]

    expected = int(
        rows(f"SELECT count() AS n FROM {fact}")[0]["n"]
    )
    aggregate_count = int(
        rows(f"SELECT count() AS n FROM {agg}")[0]["n"]
    )

    invalid = (
        result["n"] != result["unique_n"]
        or int(result["n"]) != aggregate_count
        or int(result["trips"]) != expected
        or int(result["bad"]) != 0
    )

    if invalid:
        raise RuntimeError(
            f"Invalid market corridor: {result}"
        )

    return result


def publish(ctx):
    """Атомарная публикация с защитой от повторного обмена."""
    target = f"{DB}.{TARGET}"
    ready = ctx["tables"][2]

    current = rows(
        f"""
        SELECT comment
        FROM system.tables
        WHERE database = '{DB}'
          AND name = '{TARGET}'
        """
    )[0]["comment"]

    # Если обмен прошёл, но ответ потерялся, retry не поменяет
    # старую и новую витрины местами обратно.
    if current == ctx["tag"]:
        return "already_published"

    ch(
        f"ALTER TABLE {ready} "
        f"MODIFY COMMENT '{ctx['tag']}'"
    )
    ch(f"EXCHANGE TABLES {target} AND {ready}")

    return "published"


def cleanup(ctx):
    """Удаление staging только после успешной публикации."""
    for table in ctx["tables"]:
        ch(f"DROP TABLE IF EXISTS {table}")