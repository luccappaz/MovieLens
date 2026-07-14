import os
import time
import pandas as pd
import requests
import boto3
from io import BytesIO
from dotenv import load_dotenv
import sys
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from spark_config import get_spark_session

# ==========================================================
# CONFIGURAÇÕES (MINIO E TMDB)
# ==========================================================
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
MINIO_USER = "admin"
MINIO_PASSWORD = "password"
BUCKET_NAME = "warehouse"

load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3/movie"

# Initializing S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASSWORD,
)

spark = get_spark_session("Fetching Overviews")
links_df = (
    spark.read.table("movielens.silver.links")
    .select("movieId", "tmdbId")
    .dropna(subset=["tmdbId"])
)

limit = 100
if limit:
    links_df = links_df.limit(limit)

links_df.repartition(2)

tmdb_id = 1339713
url = f"{TMDB_BASE_URL}/{tmdb_id}?api_key={TMDB_API_KEY}&language=pt-BR"
response = requests.get(url)

data = response.json()
print(data["overview"])
