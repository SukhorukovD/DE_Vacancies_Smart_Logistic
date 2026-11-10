#!/usr/bin/env bash
set -euo pipefail
bootstrap_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
sql_dir="${SQL_DIR:-/sql}"
n_prices="${N_PRICES:-300000}"
[[ "$n_prices" =~ ^[1-9][0-9]*$ ]] || { echo 'N_PRICES must be a positive integer' >&2; exit 1; }
psql_args=(-X -v ON_ERROR_STOP=1)

psql "${psql_args[@]}" -d postgres -f "$bootstrap_dir/databases.sql"
source_ready="$(psql "${psql_args[@]}" -d deals_bigdata -Atc "SELECT to_regclass('public.prices') IS NOT NULL")"
if [[ "$source_ready" == f ]]; then
    # Schema + generator are atomic: a failed first initialization is retryable.
    psql "${psql_args[@]}" -d deals_bigdata --single-transaction \
        -v n_prices="$n_prices" -f "$sql_dir/schema.sql" -f "$sql_dir/synthetic.sql"
else
    table_count="$(psql "${psql_args[@]}" -d deals_bigdata -Atc "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename IN ('cities','body_types','exchange_rates','prices','price_dates','price_body_types','price_participants','price_group_stats','favorite_directions','selected_directions','migrations')")"
    [[ "$table_count" == 11 ]] || { echo 'Source schema is incomplete; no data was reset.' >&2; exit 1; }
    source_rows="$(psql "${psql_args[@]}" -d deals_bigdata -Atc 'SELECT count(*) FROM public.prices')"
    [[ "$source_rows" != 0 ]] || { echo 'Existing source has no prices. Initialize it explicitly; no data was reset.' >&2; exit 1; }
    echo "Keeping existing source: $source_rows prices"
fi

# Explicit allowlist: never execute reset/diagnose/repair files automatically.
for migration in core_model.sql 001_prepare_core.sql 004_task_pipeline.sql; do
    psql "${psql_args[@]}" -d dwh -f "$sql_dir/$migration"
done
psql "${psql_args[@]}" -d deals_bigdata -At -f "$bootstrap_dir/raw_schema.sql" > /tmp/logistics_raw_schema.sql
psql "${psql_args[@]}" -d dwh --single-transaction -f /tmp/logistics_raw_schema.sql

# These directories are mounted only in the bootstrap service and Airflow.
for directory in "${AIRFLOW_LOG_DIR:-}" "${AIRFLOW_STATE_DIR:-}"; do
    if [[ -n "$directory" ]]; then
        mkdir -p "$directory"
        chown 50000:0 "$directory"
        chmod 2775 "$directory"
    fi
done
echo 'Database bootstrap completed; Airflow can initialize its metadata.'
