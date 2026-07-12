from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

default_args = {
    "owner": "lucca",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "movielens_silver_pipeline",
    default_args=default_args,
    description="Pipeline End-to-End for the Silver layer",
    schedule=None,
    catchup=False,
    tags=["lakehouse", "spark", "silver"],
) as dag:
    process_movies = SparkSubmitOperator(
        task_id="process_movies_silver",
        application="/opt/spark/transformations/movies.py",
        conn_id="spark_default",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

    process_ratings = SparkSubmitOperator(
        task_id="process_ratings_silver",
        application="/opt/spark/transformations/ratings.py",
        conn_id="spark_default",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )
    process_tags = SparkSubmitOperator(
        task_id="process_tags_silver",
        application="/opt/spark/transformations/tags.py",
        conn_id="spark_default",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )
    process_links = SparkSubmitOperator(
        task_id="process_links_silver",
        application="/opt/spark/transformations/links.py",
        conn_id="spark_default",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

    process_movies >> process_ratings >> process_tags >> process_links  # type: ignore
