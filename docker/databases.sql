SELECT format('CREATE DATABASE %I', name)
FROM (VALUES ('deals_bigdata'), ('dwh'), ('airflow')) AS db(name)
WHERE NOT EXISTS(SELECT 1 FROM pg_database WHERE datname=db.name)
\gexec
