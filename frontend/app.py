import sys
import os
from pyspark.sql import SparkSession
import streamlit as st
import requests
import psycopg2
import pandas as pd

try:
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    root_dir = os.getcwd()
if root_dir not in sys.path:
    sys.path.append(root_dir)

from spark_config import get_spark_session

st.set_page_config("MovieLens AI", page_icon="🎬", layout="wide")

OLLAMA_API_URL = os.environ.get(
    "OLLAMA_API_URL", "http://localhost:11434/api/embeddings"
)
OLLAMA_EMBED_MODEL = "nomic-embed-text"

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = "5432"
PG_USER = "airflow"
PG_PASS = "airflow"
PG_DB = "airflow"


@st.cache_resource
def init_spark() -> SparkSession:
    spark = get_spark_session("MovieLens-Streamlit")
    return spark


spark = init_spark()


def get_recommendations(user_id: int):
    """Procura as recomendações pré-calculadas na Camada Gold"""
    try:
        return spark.sql(f"""
            SELECT l.imdbId, m.title, m.genres, m.year, rec.score
            FROM movielens.silver.movies as m
            INNER JOIN movielens.gold.als_recommendations as rec
            ON m.movieId = rec.movieId
            INNER JOIN movielens.silver.links as l
            ON rec.movieId = l.movieId
            WHERE rec.userId = {user_id}
            ORDER BY rec.score DESC
            LIMIT 10
        """).toPandas()
    except Exception as e:
        st.error(f"Erro ao consultar a camada Gold: {e}")
        return None


def get_user_history(user_id: int):
    """Procura os filmes que o utilizador já avaliou no passado (Camada Silver)"""
    try:
        return spark.sql(f"""
            SELECT l.imdbId, m.title, m.year, m.genres, r.rating, r.timestamp
            FROM movielens.silver.ratings as r
            INNER JOIN movielens.silver.movies as m ON r.movieId = m.movieId
            INNER JOIN movielens.silver.links as l ON r.movieId = l.movieId
            WHERE r.userId = {user_id}
            ORDER BY r.timestamp DESC
        """).toPandas()
    except Exception as e:
        st.error(f"Erro ao consultar o histórico: {e}")
        return None


def semantic_search(query_text: str, limit: int = 5):
    try:
        response = requests.post(
            OLLAMA_API_URL, json={"model": OLLAMA_EMBED_MODEL, "prompt": query_text}
        )
        if response.status_code != 200:
            st.error(f"Erro no Ollama: Erro {response.status_code}")
            return None
        query_vector = response.json().get("embedding")
    except Exception as e:
        st.error(f"Falha ao conectar-se com o Ollama local: {e}")
        return None

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
        )
        cur = conn.cursor()

        cur.execute(
            """ 
            SELECT title, genres, overview, 1 - (embedding <=> %s::vector) AS similarity
            FROM movie_embeddings
            ORDER BY embedding <=> %s::vector
            LIMIT %s;
        """,
            (query_vector, query_vector, limit),
        )

        results = cur.fetchall()
        cur.close()
        conn.close()

        return pd.DataFrame(
            results, columns=["Filmes", "Gêneros", "Sinopse", "Similaridade"]
        )
    except Exception as e:
        st.error(f"Erro ao consultar o Banco Vetorial (Postgres): {e}")
        return None


# Interface gráfica ######################################################################
st.title("🎬 Sistema de Recomendação MovieLens (ALS + IA)")
st.markdown("---")

tab1, tab2 = st.tabs(
    ["🍿 Recomendações Clássicas (ALS)", "🤖 Chatbot RAG (Busca Semântica)"]
)

with tab1:
    st.sidebar.header("Configurações (ALS)")
    user_id_input = st.sidebar.number_input(
        "ID do Utilizador (ALS):", min_value=1, value=1, step=1
    )

    if st.button("Gerar Recomendações ALS", type="primary"):
        with st.spinner("A carregar dados do Lakehouse (Iceberg)..."):
            df_recs = get_recommendations(user_id_input)
            df_history = get_user_history(user_id_input)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🍿 Recomendações (Camada Gold)")
            if df_recs is not None and not df_recs.empty:
                st.dataframe(
                    df_recs,
                    column_config={
                        "title": "Filme",
                        "genres": "Géneros",
                        "year": "Ano do Filme",
                        "score": st.column_config.ProgressColumn(
                            "Afinidade (Score)",
                            help="Score de previsão do algoritmo ALS",
                            min_value=0,
                            max_value=5,
                            format="%.2f",
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.warning("Nenhuma recomendação encontrada.")

        with col2:
            st.subheader("📜 Histórico (Camada Silver)")
            if df_history is not None and not df_history.empty:
                st.dataframe(
                    df_history,
                    column_config={
                        "title": "Filme",
                        "genres": "Géneros",
                        "rating": "Nota dada",
                        "year": "Ano do Filme",
                    },
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("Este utilizador não tem histórico.")

with tab2:
    st.markdown("""
        ### Converse com o nosso Catálogo de Filmes
        Em vez de procurar pelo título exato, descreva o que lhe apetece ver! O modelo de IA irá perceber o significado e encontrar filmes com base na sinopse.
    """)

    user_query = st.text_input(
        "O que lhe apetece assistir hoje?",
        placeholder="Ex: Um grupo de amigos viaja no tempo e tudo corre mal...",
    )

    if st.button("Buscar com IA 🧠", type="primary"):
        if user_query:
            with st.spinner("A vetorizar a sua pergunta e a varrer o PostgreSQL..."):
                df_rag = semantic_search(user_query)

                if df_rag is not None and not df_rag.empty:
                    st.success("Encontrámos estes filmes com base na sua descrição!")

                    df_rag["Similaridade"] = (df_rag["Similaridade"] * 100).apply(
                        lambda x: f"{x:.1f}%"
                    )

                    st.dataframe(
                        df_rag,
                        column_config={
                            "Filme": st.column_config.TextColumn(
                                "Filme", width="medium"
                            ),
                            "Sinopse": st.column_config.TextColumn(
                                "Sinopse", width="large"
                            ),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
        else:
            st.warning("Por favor, digite alguma coisa na caixa de texto!")
