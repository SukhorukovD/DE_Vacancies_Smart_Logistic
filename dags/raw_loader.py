
import hashlib
import json
import logging
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from contextlib import contextmanager


@contextmanager
def managed_connection(config):
    conn = get_connection(config)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

logger = logging.getLogger(__name__)

SOURCE = {
    "host": "postgres",
    "port": 5432,
    "dbname": "deals_bigdata",
    "user": "postgres",
    "password": "postgres",
}

TARGET = {
    "host": "postgres",
    "port": 5432,
    "dbname": "dwh",
    "user": "postgres",
    "password": "postgres",
}

TABLES = [
    "body_types",
    "cities",
    "exchange_rates",
    "favorite_directions",
    "migrations",
    "price_body_types",
    "price_dates",
    "price_group_stats",
    "price_participants",
    "prices",
    "selected_directions",
]

BATCH_SIZE = 5000


def make_hash(row):
    payload = json.dumps(
        row,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_connection(config):
    return psycopg2.connect(**config)


def start_batch():
    batch_id = str(uuid.uuid4())

    logger.info("Starting RAW ingestion batch: %s", batch_id)

    with managed_connection(TARGET) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.etl_batches (batch_id, status)
                VALUES (%s, 'running')
                """,
                (batch_id,),
            )

    logger.info("Batch registered successfully: %s", batch_id)
    return batch_id


def load_table(table, batch_id):
    if table not in TABLES:
        raise ValueError(f"Table is not allowed: {table}")

    logger.info(
        "Starting table load: table=%s batch_id=%s",
        table,
        batch_id,
    )

    target_conn = None
    source_conn = None

    try:
        target_conn = get_connection(TARGET)

        # Регистрируем задачу. При retry используем прежнюю запись аудита.
        with target_conn.cursor() as dst:
            dst.execute(
                """
                INSERT INTO audit.etl_table_runs
                    (batch_id, source_table, status)
                VALUES (%s, %s, 'running')
                ON CONFLICT (batch_id, source_table)
                DO UPDATE SET
                    status = 'running',
                    started_at = now(),
                    finished_at = NULL,
                    error_message = NULL
                """,
                (batch_id, table),
            )
        target_conn.commit()

        source_conn = get_connection(SOURCE)

        # Получаем названия колонок.
        with source_conn.cursor() as metadata_cursor:
            metadata_cursor.execute(
                sql.SQL("SELECT * FROM public.{} LIMIT 0").format(
                    sql.Identifier(table)
                )
            )
            columns = [
                description.name
                for description in metadata_cursor.description
            ]

        if "id" not in columns:
            raise RuntimeError(f"Primary key id not found in {table}")

        id_index = columns.index("id")

        # Потоковое чтение исходной таблицы.
        cursor_name = f"raw_{table}_{uuid.uuid4().hex}"

        with source_conn.cursor(name=cursor_name) as src:
            src.itersize = BATCH_SIZE

            src.execute(
                sql.SQL("SELECT * FROM public.{} ORDER BY id").format(
                    sql.Identifier(table)
                )
            )

            insert_columns = columns + ["_batch_id", "_record_hash"]

            insert_query = sql.SQL(
                """
                INSERT INTO raw.{} ({})
                VALUES %s
                ON CONFLICT (id, _batch_id)
                DO UPDATE SET
                    {}
                """
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(
                    sql.Identifier(column)
                    for column in insert_columns
                ),
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(column),
                        sql.Identifier(column),
                    )
                    for column in columns
                ) + sql.SQL(
                    ", _loaded_at = now(), _record_hash = EXCLUDED._record_hash"
                ),
            ).as_string(target_conn)

            total_read = 0
            total_inserted = 0
            total_skipped = 0

            while True:
                rows = src.fetchmany(BATCH_SIZE)

                if not rows:
                    break

                total_read += len(rows)
                ids = [row[id_index] for row in rows]

                with target_conn.cursor() as dst:
                    # Находим последнюю известную версию каждого id.
                    dst.execute(
                        sql.SQL(
                            """
                            SELECT DISTINCT ON (id)
                                id, _record_hash
                            FROM raw.{}
                            WHERE id = ANY(%s)
                            ORDER BY id, _loaded_at DESC, _batch_id DESC
                            """
                        ).format(sql.Identifier(table)),
                        (ids,),
                    )

                    latest_hashes = {
                        record_id: record_hash
                        for record_id, record_hash in dst.fetchall()
                    }

                    new_versions = []

                    for row in rows:
                        record = tuple(row)
                        record_id = record[id_index]
                        record_hash = make_hash(record)

                        if latest_hashes.get(record_id) != record_hash:
                            new_versions.append(
                                record + (batch_id, record_hash)
                            )

                    if new_versions:
                        execute_values(
                            dst,
                            insert_query,
                            new_versions,
                            page_size=1000,
                        )

                    inserted = len(new_versions)
                    skipped = len(rows) - inserted

                    total_inserted += inserted
                    total_skipped += skipped

                    # Статистика фиксируется в той же транзакции,
                    # что и данные текущего чанка.
                    dst.execute(
                        """
                        UPDATE audit.etl_table_runs
                        SET rows_read = rows_read + %s,
                            rows_inserted = rows_inserted + %s,
                            rows_skipped = rows_skipped + %s
                        WHERE batch_id = %s
                          AND source_table = %s
                        """,
                        (
                            len(rows),
                            inserted,
                            skipped,
                            batch_id,
                            table,
                        ),
                    )

                target_conn.commit()

                logger.info(
                    "Table=%s | read=%s | inserted=%s | skipped=%s",
                    table,
                    total_read,
                    total_inserted,
                    total_skipped,
                )

        with target_conn.cursor() as dst:
            dst.execute(
                """
                UPDATE audit.etl_table_runs
                SET status = 'success',
                    finished_at = now()
                WHERE batch_id = %s AND source_table = %s
                """,
                (batch_id, table),
            )

        target_conn.commit()

        logger.info(
            "Table load completed: table=%s read=%s inserted=%s skipped=%s",
            table,
            total_read,
            total_inserted,
            total_skipped,
        )

        return {
            "table": table,
            "rows_read": total_read,
            "rows_inserted": total_inserted,
            "rows_skipped": total_skipped,
        }

    except Exception as error:
        logger.exception(
            "Table load failed: table=%s batch_id=%s",
            table,
            batch_id,
        )

        if target_conn is not None:
            target_conn.rollback()

            try:
                with target_conn.cursor() as dst:
                    dst.execute(
                        """
                        UPDATE audit.etl_table_runs
                        SET status = 'failed',
                            finished_at = now(),
                            error_message = %s
                        WHERE batch_id = %s
                          AND source_table = %s
                        """,
                        (str(error), batch_id, table),
                    )
                target_conn.commit()
            except Exception:
                target_conn.rollback()
                logger.exception("Could not update table audit status")

        raise

    finally:
        if source_conn is not None:
            source_conn.close()

        if target_conn is not None:
            target_conn.close()


def finish_batch(batch_id):
    logger.info("Finishing batch: %s", batch_id)

    with managed_connection(TARGET) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status = 'success'),
                    COUNT(*) FILTER (WHERE status = 'failed')
                FROM audit.etl_table_runs
                WHERE batch_id = %s
                """,
                (batch_id,),
            )

            total, successful, failed = cur.fetchone()

            status = (
                "success"
                if total == len(TABLES) and successful == len(TABLES)
                else "failed"
            )

            cur.execute(
                """
                UPDATE audit.etl_batches
                SET status = %s,
                    finished_at = now()
                WHERE batch_id = %s
                """,
                (status, batch_id),
            )

    logger.info(
        "Batch finished: batch_id=%s status=%s "
        "tables_total=%s tables_success=%s tables_failed=%s",
        batch_id,
        status,
        total,
        successful,
        failed,
    )
