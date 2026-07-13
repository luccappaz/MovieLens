from spark_config import get_spark_session
from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    datediff,
    round,
    count,
    log10,
    row_number,
    collect_list,
    struct,
    to_date,
    max,
    lit,
)
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS


def main() -> None:

    spark = get_spark_session("ALS_Recommendation")

    try:
        ratings_df = (
            spark.read.table("movielens.movielens.ratings")
            .withColumnRenamed("userId", "user_id")
            .withColumnRenamed("movieId", "movie_id")
        )

        # Calculando o Freshness (Decaimento Exponencial...)
        ratings_df = ratings_df.withColumn("rating_date", to_date(col("timestamp")))

        # Pegando a data mais recente do banco de dados
        max_date = ratings_df.select(max("rating_date")).collect()[0][0]
        # Fazendo a diferença para a data selecionada
        ratings_df = ratings_df.withColumn(
            "days_old", datediff(lit(max_date), col("rating_date"))
        )

        # A fórmula de decaimento da nota vai ser de meia vida em 365 dias
        ratings_df = ratings_df.withColumn(
            "fresh_rating",
            round(col("rating") / (2 ** (col("days_old") / (4 * 365))), 1),
        )

        # Verificando o Fairness (ajustando pela popularidade)
        item_popularity = ratings_df.groupBy("movie_id").agg(
            count("*").alias("num_ratings")
        )

        # Treinando o modelo ALS
        als = ALS(
            rank=10,
            maxIter=10,
            regParam=0.1,
            userCol="user_id",
            itemCol="movie_id",
            ratingCol="fresh_rating",
            coldStartStrategy="drop",
            nonnegative=True,
        )

        model = als.fit(ratings_df)

        # Vamos pedir 50 candidatos para margem de re-ranking
        raw_recommendations = model.recommendForAllUsers(50)

        # Explodindo as recomendações e separando em colunas
        recs_exploded = raw_recommendations.select(
            "user_id", explode("recommendations").alias("rec")
        ).select(
            "user_id",
            col("rec.movie_id").alias("movie_id"),
            col("rec.rating").alias("als_score"),
        )

        # Cruzando com a tabela de popularidade
        recs_joined = recs_exploded.join(
            item_popularity, on="movie_id", how="left"
        ).fillna({"num_ratings": 1})

        # Agora vamos usar penalização algorítmica suave para penalizar os filmes mais populares
        recs_scored = recs_joined.withColumn(
            "fairness_score", col("als_score") / log10(col("num_ratings") + 9.0)
        )

        # Fazemos o ranking particionando pelo "user_id" e ordendado de modo decrescente pelo "fairness_score"
        windowSpec = Window.partitionBy("user_id").orderBy(col("fairness_score").desc())
        # Assim, pegamos apenas o 10 melhores
        top_recs = recs_scored.withColumn("rank", row_number().over(windowSpec)).filter(
            col("rank") <= 10
        )

        gold_df = (
            top_recs.groupBy("user_id")
            .agg(
                collect_list(
                    struct("movie_id", col("fairness_score").alias("score"))
                ).alias("recommendations")
            )
            .withColumn("updated_at", current_timestamp())
        )
        spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

        gold_df.write.format("iceberg").mode("overwrite").saveAsTable(
            "movielens.gold.user_recommendations"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
