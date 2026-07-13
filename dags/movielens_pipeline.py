from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow import DAG
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.movielens_raw import ingestion as run_bronze_ingestion

ENV_VARS = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password",
}

default_args = {
    "owner": "lucca",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_spark_op(task_id: str, script: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/spark/transformations/{script}",
        env_vars=ENV_VARS,
        conn_id="spark_default",
        py_files="/opt/spark/spark_config.py",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
        pool="spark_pool",
    )


with DAG(
    "movielens_bronze_ingestion",
    default_args=default_args,
    description="Pipeline Bronze: Download do ZIP e Upload de CSVs no MinIO",
    catchup=False,
    schedule=None,
    tags=["lakehouse", "python", "bronze", "ingestion"],
) as dag_bronze:
    ingest_raw_data = PythonOperator(
        task_id="download_and_upload_movielens", python_callable=run_bronze_ingestion
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_processing",
        trigger_dag_id="movielens_silver_pipeline",
        wait_for_completion=False,
    )

    ingest_raw_data >> trigger_silver_dag  # type: ignore

with DAG(
    "movielens_silver_pipeline",
    default_args=default_args,
    description="Pipeline End-to-End for the Silver layer",
    schedule=None,
    catchup=False,
    tags=["lakehouse", "spark", "silver"],
) as dag_silver:
    process_movies = get_spark_op("process_movies_silver", "movies.py")
    process_ratings = get_spark_op("process_ratings_silver", "ratings.py")
    process_tags = get_spark_op("process_tags_silver", "tags.py")
    process_links = get_spark_op("process_links_silver", "links.py")

    trigger_gold_dag = TriggerDagRunOperator(
        task_id="process_recommendations",
        trigger_dag_id="movielens_gold_pipeline",
        wait_for_completion=False,
    )

    [process_movies, process_ratings, process_tags, process_links] >> trigger_gold_dag  # type: ignore

with DAG(
    "movielens_gold_pipeline",
    default_args=default_args,
    description="Pipeline End-to-End for the Golden layer",
    schedule=None,
    catchup=False,
    tags=["lakehouse", "spark", "gold"],
) as dag_gold:
    process_recommendations = get_spark_op(
        "process_recommendations", "als_recommendations.py"
    )
