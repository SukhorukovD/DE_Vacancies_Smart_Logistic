
import psycopg2
from psycopg2 import sql

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


def main():
    source_conn = psycopg2.connect(**SOURCE)
    target_conn = psycopg2.connect(**TARGET)

    try:
        with source_conn.cursor() as src, target_conn.cursor() as dst:
            dst.execute("CREATE SCHEMA IF NOT EXISTS raw")

            for table in TABLES:
                # Получаем типы и nullable для каждой колонки источника.
                src.execute(
                    """
                    SELECT
                        a.attname,
                        format_type(a.atttypid, a.atttypmod),
                        NOT a.attnotnull
                    FROM pg_attribute a
                    JOIN pg_class t ON t.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = 'public'
                      AND t.relname = %s
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum
                    """,
                    (table,),
                )
                columns = src.fetchall()

                if not columns:
                    raise RuntimeError(f"Таблица {table} не найдена")

                # Получаем первичный ключ источника.
                src.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class t ON t.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
                      ON TRUE
                    JOIN pg_attribute a
                      ON a.attrelid = t.oid AND a.attnum = k.attnum
                    WHERE n.nspname = 'public'
                      AND t.relname = %s
                      AND i.indisprimary
                    ORDER BY k.ord
                    """,
                    (table,),
                )
                primary_key = [row[0] for row in src.fetchall()]

                if not primary_key:
                    raise RuntimeError(f"У {table} нет первичного ключа")

                definitions = []

                for name, data_type, nullable in columns:
                    definition = sql.SQL("{} {}").format(
                        sql.Identifier(name),
                        sql.SQL(data_type),
                    )
                    if not nullable:
                        definition += sql.SQL(" NOT NULL")
                    definitions.append(definition)

                # Технические поля RAW.
                definitions.extend([
                    sql.SQL("_batch_id UUID NOT NULL"),
                    sql.SQL("_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()"),
                    sql.SQL("_source_system TEXT NOT NULL DEFAULT 'deals_bigdata'"),
                    sql.SQL("_record_hash TEXT NOT NULL"),
                ])

                # Один и тот же исходный id может иметь несколько версий.
                pk_columns = [
                    sql.Identifier(column) for column in primary_key
                ] + [sql.Identifier("_batch_id")]

                definitions.append(
                    sql.SQL("PRIMARY KEY ({})").format(
                        sql.SQL(", ").join(pk_columns)
                    )
                )

                create_query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {}.{} ({})"
                ).format(
                    sql.Identifier("raw"),
                    sql.Identifier(table),
                    sql.SQL(", ").join(definitions),
                )

                dst.execute(create_query)
                print(f"RAW table ready: {table}")

        target_conn.commit()
        print("\nAll RAW tables created successfully.")

    except Exception:
        target_conn.rollback()
        raise

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
