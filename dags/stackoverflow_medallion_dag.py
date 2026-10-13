"""StackOverflow Medallion pipeline: Bronze (dlt) -> Silver (Spark/Iceberg) -> Gold (Spark/Iceberg).

    bronze_ingest >> silver_transform >> gold_agg

- bronze_ingest   : dlt reads users.parquet from the source URL, keeps 2021-H1 and writes
                    Parquet to MinIO `bronze/users/2021/users_2021.parquet` (replace = overwrite).
- silver_transform: Spark job on the standalone cluster, `users_hist` via MERGE INTO (Nessie/Iceberg).
- gold_agg        : Spark job on the standalone cluster, `cant_post_x_user_hist` via MERGE INTO.

Prerequisite (manual, once): jobs/prepare_bronze_manual.py and jobs/silver_post_hist_manual.py.
The DAG is idempotent: Bronze is replaced, Silver/Gold use CREATE IF NOT EXISTS + MERGE.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS = "/opt/airflow/jobs"

# Client mode: the driver runs in the scheduler container, executors on spark-worker
# connect back to it by hostname. Memory sized for a ~4 GB worker.
SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executor.memory": "3g",
    "spark.cores.max": "3",
    "spark.ui.showConsoleProgress": "false",
}


def spark_task(job: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=job,
        conn_id="spark_default",
        application=f"{JOBS}/{job}.py",
        name=job,
        driver_memory="1g",
        conf=SPARK_CONF,
        retries=1,
    )


@dag(
    dag_id="stackoverflow_medallion",
    description="Bronze (dlt) -> Silver (Iceberg MERGE) -> Gold (Iceberg MERGE) on StackOverflow",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fhbd-grupo-2", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["fhbd", "medallion", "dlt", "spark", "iceberg", "nessie"],
    doc_md=__doc__,
)
def stackoverflow_medallion():
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"python {JOBS}/bronze_ingest.py",
        execution_timeout=timedelta(minutes=90),
    )
    silver_transform = spark_task("silver_transform")
    gold_agg = spark_task("gold_agg")

    bronze_ingest >> silver_transform >> gold_agg


stackoverflow_medallion()
