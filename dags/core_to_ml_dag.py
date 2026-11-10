import logging
import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def prepare(**context):
    from etl.ml.core_to_ml import prepare as initialize

    run = context["dag_run"]
    return initialize(
        run.run_id,
        (run.conf or {}).get("as_of", "2026-10-26"),
    )


def execute(stage, **context):
    from etl.ml import core_to_ml as etl

    ctx = context["ti"].xcom_pull(task_ids="prepare")
    started = time.monotonic()

    if stage in ("split", "features", "targets"):
        result = etl.build(ctx, stage)
    else:
        result = getattr(etl, stage)(ctx)

    logging.getLogger(__name__).info(
        "ml_stage=%s elapsed_seconds=%.3f",
        stage,
        time.monotonic() - started,
    )
    return result


with DAG(
    dag_id="core_to_ml",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ml", "features"],
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
            "extract",
            "split",
            "features",
            "targets",
            "validate",
            "publish",
            "cleanup",
        )
    }

    (
        start
        >> tasks["extract"]
        >> tasks["split"]
        >> [tasks["features"], tasks["targets"]]
    )

    (
        [tasks["features"], tasks["targets"]]
        >> tasks["validate"]
        >> tasks["publish"]
        >> tasks["cleanup"]
    )