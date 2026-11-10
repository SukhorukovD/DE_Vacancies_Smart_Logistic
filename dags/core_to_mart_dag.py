import logging
import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


LOG = logging.getLogger(__name__)


def prepare(**context):
    from etl.mart.core_to_mart import prepare as initialize

    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    return initialize(
        run_id=dag_run.run_id,
        as_of=conf.get("as_of", "2026-10-26"),
    )


def execute(stage, **context):
    from etl.mart import core_to_mart as etl

    ti = context["ti"]
    ctx = ti.xcom_pull(task_ids="prepare")
    started = time.monotonic()

    if stage == "check_facts":
        report = ti.xcom_pull(task_ids="load")
        result = etl.check_facts(ctx, report)
    else:
        result = getattr(etl, stage)(ctx)

    LOG.info(
        "mart_stage=%s elapsed_seconds=%.3f",
        stage,
        time.monotonic() - started,
    )
    return result


with DAG(
    dag_id="core_to_mart",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["mart", "clickhouse"],
) as dag:

    start = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
    )

    tasks = {
        name: PythonOperator(
            task_id=name,
            python_callable=execute,
            op_kwargs={"stage": name},
        )
        for name in (
            "load",
            "aggregate",
            "compare",
            "check_facts",
            "check_mart",
            "publish",
            "cleanup",
        )
    }

    start >> tasks["load"]

    tasks["load"] >> [
        tasks["aggregate"],
        tasks["check_facts"],
    ]

    (
        tasks["aggregate"]
        >> tasks["compare"]
        >> tasks["check_mart"]
    )

    [
        tasks["check_facts"],
        tasks["check_mart"],
    ] >> tasks["publish"]

    tasks["publish"] >> tasks["cleanup"]