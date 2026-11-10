"""Подготовка ML-данных. Без обучения моделей и прогнозирования."""

import logging
import os
import tempfile
from datetime import date
from uuid import NAMESPACE_URL, uuid5

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json


LOG = logging.getLogger(__name__)
ML_DB = os.getenv("ML_DB", "ml")

BODIES = {
    "body_tent": 56,
    "body_refrigerated": 4,
    "body_isothermal": 2,
    "body_flatbed": 15,
    "body_other": 0,
    "body_unknown": -1,
}

BASE = {
    "origin_city_gcid": "integer NOT NULL",
    "destination_city_gcid": "integer NOT NULL",
    "origin_region": "text NOT NULL",
    "destination_region": "text NOT NULL",
    "origin_latitude": "double precision",
    "origin_longitude": "double precision",
    "destination_latitude": "double precision",
    "destination_longitude": "double precision",
    "distance": "double precision NOT NULL",
    "total_weight": "double precision NOT NULL",
    "total_volume": "double precision NOT NULL",
    "weight_category": "text NOT NULL",
    "volume_category": "text NOT NULL",
    **{
        name: f"smallint NOT NULL CHECK ({name} IN (0,1))"
        for name in BODIES
    },
}

TIME = {
    "loading_year": "smallint",
    "loading_month": "smallint",
    "loading_dow": "smallint",
    "loading_doy": "smallint",
    **{
        f"{part}_{trig}": "double precision"
        for part in ("month", "dow", "doy")
        for trig in ("sin", "cos")
    },
}

# Только эти столбцы использовать как X при будущем обучении.
FEATURE_COLUMNS = list(BASE) + list(TIME)

CATEGORICAL_COLUMNS = [
    "origin_city_gcid",
    "destination_city_gcid",
    "origin_region",
    "destination_region",
    "weight_category",
    "volume_category",
]

META = """
    price_id bigint,
    source_price_sk bigint NOT NULL,
    source_batch_id uuid NOT NULL,
    loading_date date NOT NULL
"""

BASE_DDL = ", ".join(f"{name} {kind}" for name, kind in BASE.items())
TIME_DDL = ", ".join(
    f"{name} {kind} NOT NULL" for name, kind in TIME.items()
)

BASE_CHECKS = """
    CHECK (distance > 0 AND distance < 'Infinity'::float8),
    CHECK (total_weight >= 0 AND total_weight < 'Infinity'::float8),
    CHECK (total_volume >= 0 AND total_volume < 'Infinity'::float8),
    CHECK (origin_latitude BETWEEN -90 AND 90),
    CHECK (origin_longitude BETWEEN -180 AND 180),
    CHECK (destination_latitude BETWEEN -90 AND 90),
    CHECK (destination_longitude BETWEEN -180 AND 180),
    CHECK (
        body_tent + body_refrigerated + body_isothermal
        + body_flatbed + body_other + body_unknown > 0
    )
"""

DDL = f"""
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS ml.datasets (
    dataset_id uuid PRIMARY KEY,
    airflow_run_id text UNIQUE NOT NULL,
    as_of date NOT NULL,
    train_start date NOT NULL,
    test_start date NOT NULL,
    status text NOT NULL
        CHECK (status IN ('building', 'validated', 'published')),
    feature_columns text[] NOT NULL,
    categorical_columns text[] NOT NULL,
    source_info jsonb,
    stats jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);

CREATE TABLE IF NOT EXISTS ml.features (
    dataset_id uuid NOT NULL REFERENCES ml.datasets,
    {META},
    split text NOT NULL CHECK (split IN ('train', 'test', 'future')),
    {BASE_DDL},
    {TIME_DDL},
    {BASE_CHECKS},
    PRIMARY KEY (dataset_id, price_id),
    UNIQUE (dataset_id, source_price_sk)
);

CREATE TABLE IF NOT EXISTS ml.targets (
    dataset_id uuid NOT NULL REFERENCES ml.datasets,
    price_id bigint NOT NULL,
    target_price_rub numeric NOT NULL CHECK (
        target_price_rub > 0
        AND target_price_rub < 'Infinity'::numeric
    ),
    PRIMARY KEY (dataset_id, price_id)
);

CREATE TABLE IF NOT EXISTS ml.current_dataset (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    dataset_id uuid NOT NULL REFERENCES ml.datasets
);

CREATE INDEX IF NOT EXISTS features_split
    ON ml.features(dataset_id, split);

CREATE OR REPLACE VIEW ml.v_dataset AS
SELECT f.*, t.target_price_rub
FROM ml.features f
JOIN ml.current_dataset c USING (dataset_id)
LEFT JOIN ml.targets t USING (dataset_id, price_id);

CREATE OR REPLACE VIEW ml.v_train AS
SELECT * FROM ml.v_dataset WHERE split = 'train';

CREATE OR REPLACE VIEW ml.v_test AS
SELECT * FROM ml.v_dataset WHERE split = 'test';

CREATE OR REPLACE VIEW ml.v_future AS
SELECT * FROM ml.v_dataset WHERE split = 'future';
"""

ELIGIBLE = """
    p.loading_date >= %(train_start)s::date
    AND p.complex = false
    AND (
        p.loading_date > %(as_of)s::date
        OR (
            p.is_fact
            AND p.price_type IN (1,2,3,4,5,9)
            AND p.outlier_mark = false
        )
    )
"""

BODY_SQL = ", ".join(
    f"max((body_type_std_id={code})::int)::smallint AS {name}"
    for name, code in BODIES.items()
)

EXPORT = f"""
WITH bodies AS (
    SELECT price_sk, {BODY_SQL}
    FROM core.price_body_types
    GROUP BY price_sk
)
SELECT
    p.source_id,
    p.price_sk,
    p.batch_id,
    p.loading_date,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    coalesce(nullif(btrim(lc.region_name), ''), 'UNKNOWN'),
    coalesce(nullif(btrim(uc.region_name), ''), 'UNKNOWN'),
    lc.latitude,
    lc.longitude,
    uc.latitude,
    uc.longitude,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.weight_category,
    p.volume_category,
    {", ".join("b." + name for name in BODIES)},
    CASE
        WHEN p.loading_date <= %(as_of)s::date THEN p.price_rub
    END
FROM core.prices p
JOIN core.cities lc ON lc.gc_id = p.loading_city_gcid
JOIN core.cities uc ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN bodies b ON b.price_sk = p.price_sk
WHERE p.is_current AND {ELIGIBLE}
"""


def connect(store=False):
    options = {"dbname": ML_DB} if store else {}
    return psycopg2.connect(
        os.getenv("DWH_DSN", ""),
        **options,
        application_name="core_to_ml",
        connect_timeout=10,
    )


def published(cur, ctx):
    cur.execute(
        "SELECT status FROM ml.datasets WHERE dataset_id=%s",
        (ctx["id"],),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Dataset is not initialized")
    return row[0] == "published"


def prepare(run_id, as_of="2026-10-26"):
    as_of = date.fromisoformat(as_of).isoformat()

    conn = connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            source_db = cur.fetchone()[0]

            if ML_DB in (
                source_db, "postgres", "template0",
                "template1", "deals_bigdata", "dwh",
            ):
                raise RuntimeError("ML_DB must be a separate database")

            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s",
                (ML_DB,),
            )
            if not cur.fetchone():
                try:
                    cur.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(ML_DB)
                        )
                    )
                except psycopg2.errors.DuplicateDatabase:
                    pass

            cur.execute(
                """
                SELECT
                    (%s::date - interval '3 months')::date,
                    %s::date - 21
                """,
                (as_of, as_of),
            )
            train_start, test_start = cur.fetchone()
    finally:
        conn.close()

    dataset_id = uuid5(
        NAMESPACE_URL, "core_to_ml/" + run_id + "/" + as_of
    )

    ctx = {
        "id": str(dataset_id),
        "stage": "stage.s_" + dataset_id.hex,
        "as_of": as_of,
        "train_start": train_start.isoformat(),
        "test_start": test_start.isoformat(),
    }

    conn = connect(True)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)

            cur.execute(
                """
                INSERT INTO ml.datasets (
                    dataset_id, airflow_run_id, as_of,
                    train_start, test_start, status,
                    feature_columns, categorical_columns
                )
                VALUES (%s,%s,%s,%s,%s,'building',%s,%s)
                ON CONFLICT (dataset_id) DO NOTHING
                """,
                (
                    ctx["id"], run_id, as_of,
                    train_start, test_start,
                    FEATURE_COLUMNS, CATEGORICAL_COLUMNS,
                ),
            )

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ctx["stage"]} (
                    {META},
                    {BASE_DDL},
                    target_price_rub numeric,
                    split text,
                    {BASE_CHECKS},
                    PRIMARY KEY (price_id)
                )
            """)
    finally:
        conn.close()

    return ctx


def extract(ctx):
    dest = connect(True)

    try:
        with dest, dest.cursor() as dst:
            if published(dst, ctx):
                return "already_published"

            # Временный файл принадлежит одной задаче.
            # Между задачами данные передаются через staging в БД.
            with tempfile.TemporaryFile(mode="w+b") as spool:
                source = connect()

                try:
                    source.set_session(isolation_level="REPEATABLE READ")

                    with source, source.cursor() as cur:
                        cur.execute(
                            "SELECT pg_try_advisory_xact_lock(73021,1)"
                        )
                        if not cur.fetchone()[0]:
                            raise RuntimeError(
                                "CORE is being updated; retry after completion"
                            )

                        cur.execute(
                            "SELECT EXISTS(SELECT 1 FROM core_stage.active_run)"
                        )
                        if cur.fetchone()[0]:
                            raise RuntimeError(
                                "Finish the incomplete CORE batch first"
                            )

                        cur.execute("""
                            SELECT raw_batch_id::text, finished_at::text
                            FROM audit.raw_to_core_runs
                            WHERE status='success'
                            ORDER BY started_at DESC
                            LIMIT 1
                        """)
                        batch = cur.fetchone()
                        if not batch:
                            raise RuntimeError("No successful CORE batch")

                        cur.execute(f"""
                            SELECT
                                count(*),
                                count(*) FILTER (WHERE {ELIGIBLE}),
                                clock_timestamp()::text
                            FROM core.prices p
                            WHERE p.is_current
                        """, ctx)

                        current, expected, captured = cur.fetchone()
                        if not current or not expected:
                            raise RuntimeError(
                                "Empty CORE or empty eligible ML selection"
                            )

                        info = {
                            "current_prices": current,
                            "expected_rows": expected,
                            "core_batch_id": batch[0],
                            "core_finished_at": batch[1],
                            "snapshot_at": captured,
                        }

                        query = cur.mogrify(EXPORT, ctx).decode()
                        cur.copy_expert(
                            "COPY (" + query + ") TO STDOUT WITH (FORMAT CSV)",
                            spool,
                        )
                finally:
                    source.close()

                spool.seek(0)
                dst.execute(f'TRUNCATE TABLE {ctx["stage"]}')

                columns = (
                    "price_id,source_price_sk,source_batch_id,loading_date,"
                    + ",".join(BASE)
                    + ",target_price_rub"
                )

                dst.copy_expert(
                    f'COPY {ctx["stage"]} ({columns}) '
                    "FROM STDIN WITH (FORMAT CSV)",
                    spool,
                )

                dst.execute(f'SELECT count(*) FROM {ctx["stage"]}')
                if dst.fetchone()[0] != expected:
                    raise RuntimeError(
                        "Source joins lost rows; check CORE dimensions"
                    )

                dst.execute(
                    """
                    UPDATE ml.datasets
                    SET source_info=%s, status='building', stats=NULL
                    WHERE dataset_id=%s
                    """,
                    (Json(info), ctx["id"]),
                )

        LOG.info("ml_extract %s", info)
        return info
    finally:
        dest.close()


def build(ctx, stage):
    conn = connect(True)

    try:
        with conn, conn.cursor() as cur:
            if published(cur, ctx):
                return "already_published"

            table = ctx["stage"]

            if stage == "split":
                cur.execute(f"""
                    UPDATE {table}
                    SET split = CASE
                        WHEN loading_date < %(test_start)s::date THEN 'train'
                        WHEN loading_date <= %(as_of)s::date THEN 'test'
                        ELSE 'future'
                    END
                """, ctx)

            elif stage == "features":
                cur.execute(
                    "DELETE FROM ml.features WHERE dataset_id=%s",
                    (ctx["id"],),
                )

                cur.execute(f"""
                    INSERT INTO ml.features (
                        dataset_id, price_id, source_price_sk,
                        source_batch_id, loading_date, split,
                        {",".join(FEATURE_COLUMNS)}
                    )
                    SELECT
                        %(id)s::uuid,
                        price_id, source_price_sk, source_batch_id,
                        loading_date, split,
                        {",".join(BASE)},

                        extract(year FROM loading_date),
                        extract(month FROM loading_date),
                        extract(isodow FROM loading_date) - 1,
                        extract(doy FROM loading_date),

                        sin(2*pi()*(extract(month FROM loading_date)-1)/12),
                        cos(2*pi()*(extract(month FROM loading_date)-1)/12),

                        sin(2*pi()*(extract(isodow FROM loading_date)-1)/7),
                        cos(2*pi()*(extract(isodow FROM loading_date)-1)/7),

                        sin(2*pi()*(extract(doy FROM loading_date)-1)/365.25),
                        cos(2*pi()*(extract(doy FROM loading_date)-1)/365.25)
                    FROM {table}
                """, ctx)

            elif stage == "targets":
                cur.execute(
                    "DELETE FROM ml.targets WHERE dataset_id=%s",
                    (ctx["id"],),
                )

                cur.execute(f"""
                    INSERT INTO ml.targets (
                        dataset_id, price_id, target_price_rub
                    )
                    SELECT %(id)s::uuid, price_id, target_price_rub
                    FROM {table}
                    WHERE split IN ('train', 'test')
                """, ctx)

            else:
                raise ValueError(stage)

            n = cur.rowcount

            # Важно для первого большого запуска:
            # проверки должны планироваться по актуальной статистике.
            analyzed = table if stage == "split" else "ml." + stage
            cur.execute("ANALYZE " + analyzed)

            cur.execute(
                """
                UPDATE ml.datasets
                SET status='building', stats=NULL
                WHERE dataset_id=%s
                """,
                (ctx["id"],),
            )

        return {"rows": n}
    finally:
        conn.close()


def validate(ctx):
    conn = connect(True)

    try:
        with conn, conn.cursor() as cur:
            if published(cur, ctx):
                return "already_published"

            cur.execute(
                "SELECT source_info FROM ml.datasets WHERE dataset_id=%s",
                (ctx["id"],),
            )
            info = cur.fetchone()[0]

            cur.execute(
                """
                SELECT split, count(*)
                FROM ml.features
                WHERE dataset_id=%s
                GROUP BY split
                """,
                (ctx["id"],),
            )
            counts = dict(cur.fetchall())

            if (
                not counts.get("train")
                or not counts.get("test")
                or sum(counts.values()) != info["expected_rows"]
            ):
                raise RuntimeError(f"Unexpected split counts: {counts}")

            cur.execute(
                f'SELECT split,count(*) FROM {ctx["stage"]} GROUP BY split'
            )
            if dict(cur.fetchall()) != counts:
                raise RuntimeError(
                    "Feature counts differ from source snapshot"
                )

            cur.execute("""
                SELECT count(*)
                FROM ml.features
                WHERE dataset_id=%(id)s
                  AND (
                    loading_date < %(train_start)s::date
                    OR split <> CASE
                        WHEN loading_date < %(test_start)s::date THEN 'train'
                        WHEN loading_date <= %(as_of)s::date THEN 'test'
                        ELSE 'future'
                    END
                  )
            """, ctx)

            if cur.fetchone()[0]:
                raise RuntimeError("Time split violation")

            cur.execute(f"""
                SELECT count(*)
                FROM {ctx["stage"]} s
                LEFT JOIN ml.targets t
                    ON t.dataset_id=%(id)s
                   AND t.price_id=s.price_id
                WHERE (
                    s.split='future'
                    AND (
                        s.target_price_rub IS NOT NULL
                        OR t.price_id IS NOT NULL
                    )
                )
                OR (
                    s.split IN ('train','test')
                    AND (
                        t.price_id IS NULL
                        OR t.target_price_rub
                            IS DISTINCT FROM s.target_price_rub
                    )
                )
            """, ctx)

            if cur.fetchone()[0]:
                raise RuntimeError(
                    "Target mismatch or future target leakage"
                )

            cur.execute(
                "SELECT count(*) FROM ml.targets WHERE dataset_id=%s",
                (ctx["id"],),
            )
            targets = cur.fetchone()[0]

            if targets != counts["train"] + counts["test"]:
                raise RuntimeError("Unexpected target rows")

            cyclic = [
                name for name in TIME
                if name.endswith(("_sin", "_cos"))
            ]

            cur.execute(
                "SELECT count(*) FROM ml.features "
                "WHERE dataset_id=%s AND NOT ("
                + " AND ".join(
                    f"abs({name})<=1.000000001" for name in cyclic
                )
                + ")",
                (ctx["id"],),
            )

            if cur.fetchone()[0]:
                raise RuntimeError("Invalid cyclical features")

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='ml' AND table_name='features'
            """)

            allowed = set(FEATURE_COLUMNS + [
                "dataset_id", "price_id", "source_price_sk",
                "source_batch_id", "loading_date", "split",
            ])

            if {row[0] for row in cur.fetchall()} != allowed:
                raise RuntimeError(
                    "Feature schema differs from the allowed column list"
                )

            cur.execute("""
                SELECT
                    count(*) FILTER (
                        WHERE origin_latitude IS NULL
                           OR origin_longitude IS NULL
                           OR destination_latitude IS NULL
                           OR destination_longitude IS NULL
                    ),
                    count(*) FILTER (
                        WHERE origin_region='UNKNOWN'
                           OR destination_region='UNKNOWN'
                    )
                FROM ml.features
                WHERE dataset_id=%s
            """, (ctx["id"],))

            missing_geo, missing_region = cur.fetchone()

            stats = {
                "counts": counts,
                "targets": targets,
                "missing_geo": missing_geo,
                "missing_region": missing_region,
            }

            cur.execute(
                """
                UPDATE ml.datasets
                SET status='validated', stats=%s
                WHERE dataset_id=%s
                """,
                (Json(stats), ctx["id"]),
            )

        return stats
    finally:
        conn.close()


def publish(ctx):
    conn = connect(True)

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT status FROM ml.datasets
                WHERE dataset_id=%s FOR UPDATE
                """,
                (ctx["id"],),
            )
            status = cur.fetchone()[0]

            if status == "published":
                return "already_published"

            if status != "validated":
                raise RuntimeError("Dataset has not passed validation")

            cur.execute(
                """
                INSERT INTO ml.current_dataset(singleton,dataset_id)
                VALUES (true,%s)
                ON CONFLICT(singleton)
                DO UPDATE SET dataset_id=EXCLUDED.dataset_id
                """,
                (ctx["id"],),
            )

            cur.execute(
                """
                UPDATE ml.datasets
                SET status='published', published_at=clock_timestamp()
                WHERE dataset_id=%s
                """,
                (ctx["id"],),
            )

        return "published"
    finally:
        conn.close()


def cleanup(ctx):
    conn = connect(True)

    try:
        with conn, conn.cursor() as cur:
            if not published(cur, ctx):
                raise RuntimeError("Cannot clean unpublished snapshot")
            cur.execute(f'DROP TABLE IF EXISTS {ctx["stage"]}')
    finally:
        conn.close()