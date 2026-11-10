from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


STAGES = (
    "cities",
    "body_types",
    "exchange_rates",
    "prices",
    "price_dates",
    "price_body_types",
    "price_participants",
    "price_group_stats",
    "favorite_directions",
    "selected_directions",
    "migrations",
    "subscriber_directions",
)


def prepare_snapshot(**context):
    from etl.core.raw_to_core import connect
    from etl.core.raw_to_core_tasks import batches, prepare

    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            available = batches(cur, None)
    finally:
        conn.close()

    if not available:
        raise ValueError("Нет успешно загруженных RAW-батчей")

    pending = [
        batch for batch in available
        if batch[2] != "success"
    ]

    selected = pending[0] if pending else available[-1]
    batch_id = selected[0]

    return prepare(
        batch_id,
        context["dag_run"].run_id,
    )


def parameters(context):
    prepared = context["ti"].xcom_pull(
        task_ids="prepare_snapshot"
    )

    if not prepared or not prepared.get("batch_id"):
        raise RuntimeError("Задача prepare_snapshot не подготовила батч")

    return (
        prepared["batch_id"],
        context["dag_run"].run_id,
    )


def load_table(stage, **context):
    from etl.core.raw_to_core_tasks import execute

    return execute(stage, *parameters(context))


def finish_batch(**context):
    from etl.core.raw_to_core_tasks import finish

    receipts = context["ti"].xcom_pull(
        task_ids=[f"load_{stage}" for stage in STAGES]
    )
    return finish(
        *parameters(context),
        receipts=receipts or [],
    )


def propagate_failure():
    raise RuntimeError("An upstream RAW -> CORE task failed")


with DAG(
    dag_id="raw_to_core",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["raw", "core", "postgres"],
) as dag:
    start = PythonOperator(
        task_id="prepare_snapshot",
        python_callable=prepare_snapshot,
    )

    tasks = {
        stage: PythonOperator(
            task_id=f"load_{stage}",
            python_callable=load_table,
            op_kwargs={"stage": stage},
        )
        for stage in STAGES
    }

    finish_task = PythonOperator(
        task_id="finish_batch",
        python_callable=finish_batch,
        trigger_rule="all_done",
        retries=0,
    )

    failure = PythonOperator(
        task_id="fail_on_upstream_error",
        python_callable=propagate_failure,
        trigger_rule="one_failed",
        retries=0,
    )

    start >> list(tasks.values())

    [
        tasks["cities"],
        tasks["body_types"],
        tasks["exchange_rates"],
    ] >> tasks["prices"]

    tasks["prices"] >> [
        tasks["price_dates"],
        tasks["price_body_types"],
        tasks["price_participants"],
    ]

    tasks["body_types"] >> tasks["price_body_types"]

    for stage in (
        "price_group_stats",
        "favorite_directions",
        "selected_directions",
    ):
        [
            tasks["cities"],
            tasks["body_types"],
        ] >> tasks[stage]

    [
        tasks["favorite_directions"],
        tasks["selected_directions"],
    ] >> tasks["subscriber_directions"]

    [start, *tasks.values()] >> finish_task
    [start, *tasks.values(), finish_task] >> failure