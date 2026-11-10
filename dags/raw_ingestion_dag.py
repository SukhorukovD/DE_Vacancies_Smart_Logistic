
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from raw_loader import TABLES, start_batch, load_table, finish_batch


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="raw_ingestion",
    description="Load source PostgreSQL tables into DWH RAW layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["raw", "postgres", "etl"],
) as dag:

    start = PythonOperator(
        task_id="start_batch",
        python_callable=start_batch,
    )

    load_tasks = []

    for table in TABLES:
        task = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table,
            op_kwargs={
                "table": table,
                "batch_id": "{{ ti.xcom_pull(task_ids='start_batch') }}",
            },
        )
        load_tasks.append(task)

    finish = PythonOperator(
        task_id="finish_batch",
        python_callable=finish_batch,
        op_kwargs={
            "batch_id": "{{ ti.xcom_pull(task_ids='start_batch') }}",
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> load_tasks >> finish
