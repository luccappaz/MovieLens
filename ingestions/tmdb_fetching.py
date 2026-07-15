import os
from pathlib import Path
import time
import requests
from dotenv import load_dotenv
import sys
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

try:
    root_dir = Path(__file__).resolve().parent.parent
    sys.path.append(str(root_dir))
except NameError:
    root_dir = Path.cwd()

from spark_config import get_spark_session

# ==========================================================
# CONFIGURAÇÕES (MINIO E TMDB)
# ==========================================================
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
MINIO_USER = "admin"
MINIO_PASSWORD = "password"
BUCKET_NAME = "warehouse"

load_dotenv(root_dir / ".env")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")


def fecth_partition(iterator):  # A iterator for each partition
    session = requests.Session()

    for row in iterator:
        movieId = row.movieId
        tmdbId = row.tmdbId

        if not tmdbId:
            continue

        url = f"https://api.themoviedb.org/3/movie/{tmdbId}?language=pt-BR"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {TMDB_API_KEY}",
        }
        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                yield (
                    int(movieId),
                    int(tmdbId),
                    str(data.get("overview", "")),
                    str(data.get("poster_path", "")),
                    int(data.get("budget", 0)),
                    int(data.get("revenue", 0)),
                    float(data.get("vote_average", 0.0)),
                )
            else:
                yield (int(movieId), int(tmdbId), "", "", 0, 0, 0.0)
        except Exception:
            yield (int(movieId), int(tmdbId), "", "", 0, 0, 0.0)

        time.sleep(0.1)


def fecth_tmdb(limit: int | None = 500):
    spark = get_spark_session("Fetching tmdb details")
    try:
        links_df = (
            spark.read.table("movielens.silver.links")
            .select("movieId", "tmdbId")
            .dropna(subset=["tmdbId"])
        )

        if limit:
            links_df = links_df.limit(limit)

        links_df.repartition(2)  # Partions to run in parallel
        rdd_enriched = links_df.rdd.mapPartitions(fecth_partition)

        schema = StructType(
            [
                StructField("movieId", IntegerType(), True),
                StructField("tmdbId", IntegerType(), True),
                StructField("overview", StringType(), True),
                StructField("poster_path", StringType(), True),
                StructField("budget", IntegerType(), True),
                StructField("revenue", IntegerType(), True),
                StructField("vote_average", DoubleType(), True),
            ]
        )

        enriched_spark_df = spark.createDataFrame(rdd_enriched, schema=schema)
        enriched_spark_df.write.format("iceberg").mode("overwrite").saveAsTable(
            "movielens.gold.tmdb_details"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    fecth_tmdb()
