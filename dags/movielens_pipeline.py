import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import Asset

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestions.movielens_raw import ingestion as run_bronze_ingestion

# ------------------------------------------------------------------
# 1. Definição dos Assets (URIs lógicas ou físicas)
# ------------------------------------------------------------------
DS_BRONZE_RAW = Asset("s3://warehouse/bronze/movielens_raw")

DS_SILVER_MOVIES = Asset("iceberg://lakehouse/silver/movies")
DS_SILVER_RATINGS = Asset("iceberg://lakehouse/silver/ratings")
DS_SILVER_TAGS = Asset("iceberg://lakehouse/silver/tags")
DS_SILVER_LINKS = Asset("iceberg://lakehouse/silver/links")
DS_GOLD_TMDB_DETAILS = Asset("iceberg://lakehouse/gold/tmdb_details")

ENV_VARS = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password",
    "PYSPARK_DRIVER_PYTHON": "/opt/python3.10/bin/python3.10",
    "PYSPARK_PYTHON": "python3",
    "SPARK_HOME": "/home/airflow/.local/lib/python3.12/site-packages/pyspark"
}

default_args = {
    "owner": "lucca",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tzinfo=ZoneInfo("America/New_York")),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_spark_op(
    task_id: str, script: str, outlets: list[Asset] | None = None
) -> SparkSubmitOperator:
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
        outlets=outlets or [],  # Registra qual Asset esta task produz
    )


# ------------------------------------------------------------------
# CAMADA BRONZE
# ------------------------------------------------------------------
with DAG(
    "movielens_bronze_ingestion",
    default_args=default_args,
    description="Pipeline Bronze: Download do ZIP e Upload de CSVs no MinIO",
    catchup=False,
    schedule=None,  # Disparo manual ou via cron/agendamento
    tags=["lakehouse", "python", "bronze", "ingestion"],
) as dag_bronze:

    ingest_raw_data = PythonOperator(
        task_id="download_and_upload_movielens",
        python_callable=run_bronze_ingestion,
        outlets=[DS_BRONZE_RAW],  # Atualiza o Asset Bronze ao finalizar
    )


# ------------------------------------------------------------------
# CAMADA SILVER
# Escuta o Asset Bronze. Ao ser atualizado, a DAG é disparada.
# ------------------------------------------------------------------
with DAG(
    "movielens_silver_pipeline",
    default_args=default_args,
    description="Pipeline End-to-End for the Silver layer",
    schedule=[DS_BRONZE_RAW],  # Disparada automaticamente pelo Asset Bronze
    catchup=False,
    tags=["lakehouse", "spark", "silver"],
) as dag_silver:

    process_movies = get_spark_op(
        "process_movies_silver", "movies.py", outlets=[DS_SILVER_MOVIES]
    )
    process_ratings = get_spark_op(
        "process_ratings_silver", "ratings.py", outlets=[DS_SILVER_RATINGS]
    )
    process_tags = get_spark_op(
        "process_tags_silver", "tags.py", outlets=[DS_SILVER_TAGS]
    )
    process_links = get_spark_op(
        "process_links_silver", "links.py", outlets=[DS_SILVER_LINKS]
    )


# ------------------------------------------------------------------
# CAMADA GOLD
# Escuta TODOS os Assets Silver.
# Só dispara quando os 4 forem atualizados!
# ------------------------------------------------------------------
with DAG(
    "movielens_gold_pipeline",
    default_args=default_args,
    description="Pipeline End-to-End for the Golden layer",
    schedule=[
        DS_SILVER_MOVIES,
        DS_SILVER_RATINGS,
        DS_SILVER_TAGS,
        DS_SILVER_LINKS,
    ],
    catchup=False,
    tags=["lakehouse", "spark", "gold"],
) as dag_gold:

    process_recommendations = get_spark_op(
        "process_recommendations", "als_recommendations.py"
    )

    fetch_tmdb_details = get_spark_op(
        "fetch_tmdb_details",
        "../ingestions/tmdb_fetching.py",
        outlets=[DS_GOLD_TMDB_DETAILS],
    )

    generate_embeddings = get_spark_op(
        "generate_movie_embeddings",
        "movie_embeddings.py",
    )

    fetch_tmdb_details >> generate_embeddings  # pyright: ignore[reportUnusedExpression]
