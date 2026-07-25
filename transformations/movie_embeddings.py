import os
import sys

import psycopg2
import requests
from requests.exceptions import RequestException

try:
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    root_dir = os.getcwd()

if root_dir not in sys.path:
    sys.path.append(root_dir)

from spark_config import get_spark_session

OLLAMA_API_URL = os.environ.get(
    "OLLAMA_API_URL", "http://localhost:11434/api/embeddings"
)
OLLAMA_EMBED_MODEL = "nomic-embed-text"

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASS = "admin"
PG_DB = "lakehouse"


def get_embedding(text: str) -> list | None:
    # Consumindo a API do Ollama local
    try:
        response = requests.post(
            OLLAMA_API_URL, json={"model": OLLAMA_EMBED_MODEL, "prompt": text}
        )
        if response.status_code == 200:
            return response.json().get("embedding")
    except RequestException as e:
        print(f"❌ Falha ao conectar ao Ollama: {e}")
        return None


def init_vector_db():
    # Conectando ao Postgresql e ativando a tabela
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
    )
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_embeddings (
            movieId INTEGER PRIMARY KEY,
            title VARCHAR(255),
            genres VARCHAR(255),
            overview TEXT,
            embedding VECTOR(768)
        )
    """)
    conn.commit()
    return conn, cur


def run_vectorization():
    # Inciando o banco de dados
    pg_conn, pg_cur = init_vector_db()
    spark = get_spark_session("RAG_embedding")

    movies_df = spark.table("movielens.silver.movies")
    tmdb_df = spark.table("movielens.gold.tmdb_details")

    enriched_movies = movies_df.join(tmdb_df, how="INNER", on="movieId").filter(
        "overview IS NOT NULL AND overview != ''"
    )
    movies_to_process = enriched_movies.limit(500).collect()

    success = 0
    for row in movies_to_process:
        context_text = (
            f"Filme: {row.title}. Gêneros: {row.genres}. Sinopse: {row.overview}"
        )
        vector = get_embedding(context_text)

        if vector:
            pg_cur.execute(
                """
                INSERT INTO movie_embeddings (movieId, title, genres, overview, embedding)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (movieId) DO UPDATE
                SET embedding = EXCLUDED.embedding, overview = EXCLUDED.overview;
            """,
                (row.movieId, row.title, row.genres, row.overview, vector),
            )
            success = success + 1
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()
    spark.stop()

    print(f"✅ Concluído! {success} vetores salvos no banco de dados.")


if __name__ == "__main__":
    run_vectorization()
