from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.0"

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
        packages=SPARK_PACKAGES,
        env_vars=ENV_VARS,
        conn_id="spark_default",
        py_files="/opt/spark/spark_config.py",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

    process_ratings = SparkSubmitOperator(
        task_id="process_ratings_silver",
        application="/opt/spark/transformations/ratings.py",
        packages=SPARK_PACKAGES,
        env_vars=ENV_VARS,
        conn_id="spark_default",
        py_files="/opt/spark/spark_config.py",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )
    process_tags = SparkSubmitOperator(
        task_id="process_tags_silver",
        application="/opt/spark/transformations/tags.py",
        packages=SPARK_PACKAGES,
        env_vars=ENV_VARS,
        conn_id="spark_default",
        py_files="/opt/spark/spark_config.py",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )
    process_links = SparkSubmitOperator(
        task_id="process_links_silver",
        application="/opt/spark/transformations/links.py",
        packages=SPARK_PACKAGES,
        env_vars=ENV_VARS,
        conn_id="spark_default",
        py_files="/opt/spark/spark_config.py",
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

    process_movies >> process_ratings >> process_tags >> process_links  # type: ignore
