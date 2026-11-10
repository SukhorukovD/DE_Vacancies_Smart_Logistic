import hashlib
import json
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


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

TABLE = "prices"
BATCH_SIZE = 5000


def make_hash(row):
    """Детерминированный хеш всех исходных полей записи."""
    payload = json.dumps(
        row,
        ensure_ascii=False,
        sort_keys=False,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()



def main():
    source_conn = psycopg2.connect(**SOURCE)
    target_conn = psycopg2.connect(**TARGET)
    batch_id = uuid.uuid4()

    try:
        # Получаем названия колонок обычным курсором.
        with source_conn.cursor() as metadata_cursor:
            metadata_cursor.execute(
                sql.SQL("SELECT * FROM public.{} LIMIT 0").format(
                    sql.Identifier(TABLE)
                )
            )
            columns = [desc.name for desc in metadata_cursor.description]

        id_index = columns.index("id")

        # Открываем потоковый курсор для чтения данных.
        with source_conn.cursor(name="prices_source_cursor") as src:
            src.itersize = BATCH_SIZE
            src.execute(
                sql.SQL("SELECT * FROM public.{} ORDER BY id").format(
                    sql.Identifier(TABLE)
                )
            )

            with target_conn.cursor() as dst:
                dst.execute(
                    "INSERT INTO audit.etl_batches (batch_id) VALUES (%s)",
                    (str(batch_id),),
                )
                dst.execute(
                    """
                    INSERT INTO audit.etl_table_runs
                        (batch_id, source_table)
                    VALUES (%s, %s)
                    """,
                    (str(batch_id), TABLE),
                )
                target_conn.commit()

                insert_columns = columns + ["_batch_id", "_record_hash"]
                insert_query = sql.SQL(
                    "INSERT INTO raw.{} ({}) VALUES %s"
                ).format(
                    sql.Identifier(TABLE),
                    sql.SQL(", ").join(
                        sql.Identifier(col) for col in insert_columns
                    ),
                ).as_string(target_conn)

                total_read = 0
                total_inserted = 0

                while True:
                    rows = src.fetchmany(BATCH_SIZE)
                    if not rows:
                        break

                    total_read += len(rows)
                    ids = [row[id_index] for row in rows]

                    dst.execute(
                        """
                        SELECT DISTINCT ON (id) id, _record_hash
                        FROM raw.prices
                        WHERE id = ANY(%s)
                        ORDER BY id, _loaded_at DESC, _batch_id DESC
                        """,
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
                                record + (str(batch_id), record_hash)
                            )

                    if new_versions:
                        execute_values(
                            dst,
                            insert_query,
                            new_versions,
                            page_size=1000,
                        )

                    inserted = len(new_versions)
                    total_inserted += inserted
                    skipped = len(rows) - inserted

                    dst.execute(
                        """
                        UPDATE audit.etl_table_runs
                        SET rows_read = rows_read + %s,
                            rows_inserted = rows_inserted + %s,
                            rows_skipped = rows_skipped + %s
                        WHERE batch_id = %s AND source_table = %s
                        """,
                        (
                            len(rows),
                            inserted,
                            skipped,
                            str(batch_id),
                            TABLE,
                        ),
                    )
                    target_conn.commit()

                    print(
                        f"Read: {total_read:,} | "
                        f"Inserted: {total_inserted:,} | "
                        f"Skipped: {total_read - total_inserted:,}"
                    )

                dst.execute(
                    """
                    UPDATE audit.etl_table_runs
                    SET status = 'success', finished_at = now()
                    WHERE batch_id = %s AND source_table = %s
                    """,
                    (str(batch_id), TABLE),
                )
                dst.execute(
                    """
                    UPDATE audit.etl_batches
                    SET status = 'success', finished_at = now()
                    WHERE batch_id = %s
                    """,
                    (str(batch_id),),
                )
                target_conn.commit()

                print("\nRAW load completed.")
                print(f"Batch ID: {batch_id}")
                print(f"Rows read: {total_read:,}")
                print(f"Rows inserted: {total_inserted:,}")
                print(f"Rows skipped: {total_read - total_inserted:,}")

    except Exception as error:
        target_conn.rollback()

        with target_conn.cursor() as dst:
            dst.execute(
                """
                UPDATE audit.etl_table_runs
                SET status = 'failed',
                    finished_at = now(),
                    error_message = %s
                WHERE batch_id = %s AND source_table = %s
                """,
                (str(error), str(batch_id), TABLE),
            )
            dst.execute(
                """
                UPDATE audit.etl_batches
                SET status = 'failed',
                    finished_at = now(),
                    error_message = %s
                WHERE batch_id = %s
                """,
                (str(error), str(batch_id)),
            )
            target_conn.commit()

        raise

    finally:
        source_conn.close()
        target_conn.close()

if __name__ == "__main__":
    main()
