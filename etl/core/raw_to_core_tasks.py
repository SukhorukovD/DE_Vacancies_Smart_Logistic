import argparse
import json
import logging
from uuid import uuid4

from psycopg2.extras import Json

from . import raw_to_core as etl


PARENTS = {
    "cities": (),
    "body_types": (),
    "exchange_rates": (),
    "prices": ("cities", "body_types", "exchange_rates"),
    "price_dates": ("prices",),
    "price_body_types": ("prices", "body_types"),
    "price_participants": ("prices",),
    "price_group_stats": ("cities", "body_types"),
    "favorite_directions": ("cities", "body_types"),
    "selected_directions": ("cities", "body_types"),
    "migrations": (),
    "subscriber_directions": (
        "favorite_directions",
        "selected_directions",
    ),
}

STAGES = tuple(PARENTS)


def batches(cur, target):
    presence = " UNION ALL ".join(
        f"SELECT 1 FROM raw.{table} WHERE _batch_id = b.batch_id"
        for table in etl.TABLES
    )

    cur.execute(
        f"""
        SELECT b.batch_id::text, b.started_at, r.status
        FROM audit.etl_batches b
        LEFT JOIN audit.raw_to_core_runs r
            ON r.raw_batch_id = b.batch_id
        WHERE b.status = 'success'
          AND (
              SELECT count(*)
              FROM audit.etl_table_runs t
              WHERE t.batch_id = b.batch_id
                AND t.status = 'success'
                AND t.source_table = ANY(%s)
          ) = %s
          AND (
              b.batch_id = %s
              OR r.raw_batch_id IS NOT NULL
              OR EXISTS ({presence})
          )
        ORDER BY b.started_at, b.batch_id
        """,
        (list(etl.TABLES), len(etl.TABLES), target),
    )
    return cur.fetchall()


def lock(cur, shared=False):
    suffix = "_shared" if shared else ""
    cur.execute(
        f"SELECT pg_advisory_xact_lock{suffix}(73021, 1)"
    )


def active(cur, batch_id, owner):
    cur.execute(
        """
        SELECT raw_batch_id::text, owner_run_id
        FROM core_stage.active_run
        """
    )
    if cur.fetchone() != (batch_id, owner):
        raise RuntimeError(
            "This DAG run does not own the staged snapshot"
        )


def completed(cur, batch_id):
    cur.execute(
        """
        SELECT status
        FROM audit.raw_to_core_runs
        WHERE raw_batch_id = %s
        """,
        (batch_id,),
    )
    return cur.fetchone() == ("success",)


def prepare(batch_id, owner):
    conn = etl.connect()

    try:
        with conn, conn.cursor() as cur:
            lock(cur)

            cur.execute(
                """
                SELECT count(*)
                FROM audit.raw_to_core_runs
                WHERE pipeline_version <> %s
                """,
                (etl.PIPELINE_VERSION,),
            )
            if cur.fetchone()[0]:
                raise RuntimeError(
                    "Reset CORE before changing "
                    "the data transformation version"
                )

            plan = batches(cur, batch_id)
            position = next(
                (
                    i for i, row in enumerate(plan)
                    if row[0] == batch_id
                ),
                None,
            )

            if position is None:
                raise ValueError(
                    "RAW batch must have all 11 successful table runs"
                )

            if completed(cur, batch_id):
                return {
                    "batch_id": batch_id,
                    "owner": owner,
                    "already_processed": True,
                }

            if any(row[2] != "success" for row in plan[:position]):
                raise RuntimeError(
                    "Load earlier RAW batches first, "
                    "in chronological order"
                )

            if any(row[2] == "success" for row in plan[position + 1:]):
                raise RuntimeError(
                    "A late historical batch requires an explicit rebuild"
                )

            cur.execute(
                "SELECT raw_batch_id::text FROM core_stage.active_run"
            )
            previous = cur.fetchone()

            if previous and previous[0] != batch_id:
                raise RuntimeError(
                    f"Finish or reset incomplete batch {previous[0]} first"
                )

            if previous:
                cur.execute(
                    """
                    UPDATE core_stage.active_run
                    SET owner_run_id = %s
                    """,
                    (owner,),
                )
                cur.execute(
                    """
                    UPDATE audit.raw_to_core_runs
                    SET status = 'running',
                        error_message = NULL,
                        finished_at = NULL
                    WHERE raw_batch_id = %s
                    """,
                    (batch_id,),
                )
                return {"batch_id": batch_id, "owner": owner}

            cur.execute(
                """
                SELECT count(*)
                FROM audit.core_task_runs
                WHERE raw_batch_id = %s
                """,
                (batch_id,),
            )
            if cur.fetchone()[0]:
                raise RuntimeError(
                    "Incomplete task checkpoint has no snapshot; reset CORE"
                )

            cur.execute(
                """
                INSERT INTO audit.raw_to_core_runs (
                    raw_batch_id, source_started_at,
                    status, pipeline_version
                )
                VALUES (%s, %s, 'running', %s)
                ON CONFLICT (raw_batch_id) DO UPDATE SET
                    status = 'running',
                    started_at = clock_timestamp(),
                    finished_at = NULL,
                    summary = NULL,
                    error_message = NULL,
                    pipeline_version = EXCLUDED.pipeline_version
                """,
                (batch_id, plan[position][1], etl.PIPELINE_VERSION),
            )

            cur.execute(
                """
                INSERT INTO core_stage.active_run (
                    raw_batch_id, owner_run_id
                )
                VALUES (%s, %s)
                """,
                (batch_id, owner),
            )

            cur.executemany(
                """
                INSERT INTO audit.core_task_runs (
                    raw_batch_id, task_name, status
                )
                VALUES (%s, %s, 'pending')
                """,
                [(batch_id, stage) for stage in STAGES],
            )

            cur.execute(
                "SET LOCAL search_path = core_stage, public"
            )

            for table in etl.TABLES:
                cur.execute(
                    f"DROP TABLE IF EXISTS core_stage.s_{table}"
                )

            etl.snapshot(cur, batch_id)

        return {"batch_id": batch_id, "owner": owner}

    finally:
        conn.close()


def transform(cur, stage, batch_id, observed_at):
    physical = None

    if stage == "cities":
        etl.load_cities(cur)

    elif stage == "body_types":
        etl.load_body_types(cur)

    elif stage == "exchange_rates":
        physical = etl.rates(cur, batch_id, observed_at)

    elif stage == "prices":
        physical = etl.prices(cur, batch_id, observed_at)

    elif stage in (
        "price_dates",
        "price_body_types",
        "price_participants",
    ):
        etl.children(cur, stage)

    elif stage in (
        "favorite_directions",
        "selected_directions",
    ):
        etl.validate_directions(cur, stage)

    elif stage == "price_group_stats":
        etl.load_group_stats(cur, batch_id, observed_at)

    elif stage == "migrations":
        etl.validate_migrations(cur)

    elif stage == "subscriber_directions":
        etl.load_directions(cur, batch_id)
        return {"materialized": True}

    result = etl.save_results(cur, batch_id, stage)[stage]

    if physical is not None:
        result["scd2"] = physical

    return result


def execute(stage, batch_id, owner):
    if stage not in STAGES:
        raise ValueError(f"Unknown stage: {stage}")

    conn = etl.connect()

    try:
        with conn, conn.cursor() as cur:
            lock(cur, shared=True)

            if completed(cur, batch_id):
                return {
                    "stage": stage,
                    "batch_id": batch_id,
                    "owner": owner,
                }

            active(cur, batch_id, owner)

            cur.execute(
                """
                SELECT status
                FROM audit.core_task_runs
                WHERE raw_batch_id = %s AND task_name = %s
                FOR UPDATE
                """,
                (batch_id, stage),
            )
            row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    "prepare_snapshot did not register this task"
                )

            if row[0] == "success":
                return {
                    "stage": stage,
                    "batch_id": batch_id,
                    "owner": owner,
                }

            cur.execute(
                """
                SELECT count(*)
                FROM audit.core_task_runs
                WHERE raw_batch_id = %s
                  AND task_name = ANY(%s)
                  AND status = 'success'
                """,
                (batch_id, list(PARENTS[stage])),
            )
            if cur.fetchone()[0] != len(PARENTS[stage]):
                raise RuntimeError(
                    f"Incomplete dependencies for {stage}"
                )

            cur.execute(
                """
                SELECT started_at
                FROM audit.raw_to_core_runs
                WHERE raw_batch_id = %s
                """,
                (batch_id,),
            )
            observed_at = cur.fetchone()[0]

            cur.execute(
                "SET LOCAL search_path = core_stage, public"
            )

            summary = transform(
                cur, stage, batch_id, observed_at
            )

            cur.execute(
                """
                UPDATE audit.core_task_runs
                SET status = 'success',
                    finished_at = clock_timestamp(),
                    summary = %s,
                    error_message = NULL
                WHERE raw_batch_id = %s AND task_name = %s
                """,
                (Json(summary), batch_id, stage),
            )

        etl.LOG.info(
            "core_task batch=%s stage=%s summary=%s",
            batch_id,
            stage,
            json.dumps(summary),
        )

        return {
            "stage": stage,
            "batch_id": batch_id,
            "owner": owner,
        }

    except Exception as exc:
        try:
            with conn, conn.cursor() as cur:
                lock(cur, shared=True)
                active(cur, batch_id, owner)

                cur.execute(
                    """
                    UPDATE audit.core_task_runs
                    SET status = 'failed',
                        finished_at = clock_timestamp(),
                        error_message = %s
                    WHERE raw_batch_id = %s
                      AND task_name = %s
                      AND status <> 'success'
                    """,
                    (str(exc)[:4000], batch_id, stage),
                )
        except Exception:
            etl.LOG.exception(
                "Could not record failure of %s", stage
            )
        raise

    finally:
        conn.close()


def finish(batch_id, owner, receipts):
    expected = {
        (stage, batch_id, owner)
        for stage in STAGES
    }
    received = {
        (r.get("stage"), r.get("batch_id"), r.get("owner"))
        for r in receipts
        if isinstance(r, dict)
    }

    conn = etl.connect()
    failure = None

    try:
        with conn, conn.cursor() as cur:
            lock(cur)

            if completed(cur, batch_id):
                if received != expected:
                    raise RuntimeError(
                        "Some Airflow tasks did not finish successfully"
                    )
                return {"already_processed": True}

            active(cur, batch_id, owner)

            cur.execute(
                """
                SELECT task_name, status, summary
                FROM audit.core_task_runs
                WHERE raw_batch_id = %s
                """,
                (batch_id,),
            )
            rows = cur.fetchall()

            successful = {
                name for name, status, _ in rows
                if status == "success"
            }

            if received != expected or successful != set(STAGES):
                failure = (
                    "Missing, failed or unfinished CORE tasks; "
                    "resume the same RAW batch"
                )
                cur.execute(
                    """
                    UPDATE audit.raw_to_core_runs
                    SET status = 'failed',
                        finished_at = clock_timestamp(),
                        error_message = %s
                    WHERE raw_batch_id = %s
                    """,
                    (failure, batch_id),
                )
            else:
                summary = {
                    name: {
                        key: value
                        for key, value in stats.items()
                        if key != "scd2"
                    }
                    for name, _, stats in rows
                    if name in etl.TABLES
                }
                summary["scd2"] = {
                    name: stats["scd2"]
                    for name, _, stats in rows
                    if stats and "scd2" in stats
                }

                cur.execute(
                    """
                    UPDATE audit.raw_to_core_runs
                    SET status = 'success',
                        finished_at = clock_timestamp(),
                        summary = %s,
                        error_message = NULL
                    WHERE raw_batch_id = %s
                    """,
                    (Json(summary), batch_id),
                )
                cur.execute(
                    "DELETE FROM core_stage.active_run"
                )

        if failure:
            raise RuntimeError(failure)

        return summary

    finally:
        conn.close()


def run(batch_id):
    conn = etl.connect()

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM audit.raw_to_core_runs
                WHERE pipeline_version <> %s
                """,
                (etl.PIPELINE_VERSION,),
            )
            if cur.fetchone()[0]:
                raise RuntimeError(
                    "Reset CORE before changing "
                    "the data transformation version"
                )
            plan = batches(cur, batch_id)
    finally:
        conn.close()

    position = next(
        (
            i for i, row in enumerate(plan)
            if row[0] == batch_id
        ),
        None,
    )
    if position is None:
        raise ValueError("Unknown or incomplete RAW batch")

    results = {}

    for current, _, status in plan[:position + 1]:
        if status == "success":
            continue

        owner = "cli-" + str(uuid4())
        prepare(current, owner)
        receipts = []

        try:
            for stage in STAGES:
                receipts.append(
                    execute(stage, current, owner)
                )
        except Exception:
            try:
                finish(current, owner, receipts)
            except Exception:
                pass
            raise

        results[current] = finish(
            current, owner, receipts
        )

    return {
        "processed": results,
        "already_processed": not results,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-batch-id", required=True)

    print(
        json.dumps(
            run(parser.parse_args().raw_batch_id),
            ensure_ascii=False,
            indent=2,
        )
    )