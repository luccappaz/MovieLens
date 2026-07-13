import streamlit as st
from pyspark.sql import SparkSession
from spark_config import get_spark_session

# Configuração da página do Streamlit
st.set_page_config(page_title="MovieLens Recs", page_icon="🎬", layout="wide")


# ==========================================================
# CACHE DA SESSÃO SPARK
# O @st.cache_resource garante que o Spark só inicializa uma vez
# ==========================================================
@st.cache_resource
def spark_init() -> SparkSession:
    return get_spark_session("Movielens_Streamlit")


# Inicializa o Spark
spark = spark_init()


# ==========================================================
# FUNÇÕES DE CONSULTA (DATA RECOVERY)
# ==========================================================
def get_recommendations(user_id: int):
    """Procura as recomendações pré-calculadas na Camada Gold"""
    try:
        return spark.sql(f"""
            SELECT rec.movieId, m.title, m.genres, rec.score
            FROM movielens.silver.movies as m
            INNER JOIN movielens.gold.als_recommendations as rec
            ON m.movieId = rec.movieId
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
            SELECT r.movieId, m.title, m.year, m.genres, r.rating, r.timestamp
            FROM movielens.silver.ratings as r
            INNER JOIN movielens.silver.movies as m ON r.movieId = m.movieId
            WHERE r.userId = {user_id}
            ORDER BY r.timestamp DESC
        """).toPandas()
    except Exception as e:
        st.error(f"Erro ao consultar o histórico: {e}")
        return None


# ==========================================================
# INTERFACE GRÁFICA (UI)
# ==========================================================
st.title("🎬 Sistema de Recomendação MovieLens")
st.markdown("---")

# Barra lateral para entrada de dados
st.sidebar.header("Configurações do Utilizador")
user_id_input = st.sidebar.number_input(
    "Introduza o ID do Utilizador:", min_value=1, value=1, step=1
)

if st.sidebar.button("Gerar Recomendações", type="primary"):
    with st.spinner("A carregar dados do Lakehouse..."):
        # Executa as consultas no Iceberg
        df_recs = get_recommendations(user_id_input)
        df_history = get_user_history(user_id_input)

    # Layout em duas colunas para o Painel Principal
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🍿 Recomendações do Modelo (Camada Gold)")
        if df_recs is not None and not df_recs.empty:
            # Formata a barra de progresso com base no Score do ALS
            st.dataframe(
                df_recs[["title", "genres", "score"]],
                column_config={
                    "title": "Filme",
                    "genres": "Géneros",
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
            st.warning(
                "Nenhuma recomendação encontrada para este utilizador na camada Gold."
            )

    with col2:
        st.subheader("📜 Últimos Filmes Avaliados (Camada Silver)")
        if df_history is not None and not df_history.empty:
            st.dataframe(
                df_history[["title", "genres", "rating", "year"]],
                column_config={
                    "title": "Filme",
                    "genres": "Géneros",
                    "rating": "Nota Dada",
                    "year": "Ano do Filme",
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("Este utilizador ainda não tem histórico de avaliações registado.")
else:
    st.info(
        "Escolha um ID de utilizador na barra lateral e clique em 'Gerar Recomendações' para consultar o Lakehouse."
    )
