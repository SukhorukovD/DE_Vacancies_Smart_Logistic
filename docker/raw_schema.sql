-- Run against the SOURCE database; emits DDL to be applied to dwh.
SELECT 'CREATE SCHEMA IF NOT EXISTS raw;';
SELECT format(
    'CREATE TABLE IF NOT EXISTS raw.%I (%s, _batch_id uuid NOT NULL, '
    '_loaded_at timestamptz NOT NULL DEFAULT now(), '
    '_source_system text NOT NULL DEFAULT ''deals_bigdata'', '
    '_record_hash text NOT NULL, PRIMARY KEY(id,_batch_id));',
    c.relname,
    string_agg(format('%I %s%s',a.attname,format_type(a.atttypid,a.atttypmod),
        CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END), ', ' ORDER BY a.attnum)
)
FROM pg_attribute a
JOIN pg_class c ON c.oid=a.attrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped
  AND c.relkind='r' AND c.relname IN (
    'cities','body_types','exchange_rates','prices','price_dates',
    'price_body_types','price_participants','price_group_stats',
    'favorite_directions','selected_directions','migrations'
  )
GROUP BY c.relname ORDER BY c.relname;
