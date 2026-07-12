from spark_config import get_spark_session
from pyspark.sql.functions import col, transform, current_timestamp
from pyspark.ml.recommendation import ALS
from pyspark.errors.exceptions.base import AnalysisException

spark = get_spark_session("ALS_Recommendation")
try:
    ratings_df = spark.read.table("my_catalog.movielens.ratings")
except AnalysisException as e:
    print(
        f"Silver layer can't be found. Error: {e}. \n Creating mock data frame for tests..."
    )
    data = [
        (1, 101, 5.0),
        (1, 102, 4.0),
        (1, 103, 1.0),
        (2, 101, 4.0),
        (2, 104, 5.0),
        (2, 105, 4.0),
        (3, 102, 2.0),
        (3, 103, 5.0),
        (3, 106, 5.0),
        (4, 101, 1.0),
        (4, 104, 1.0),
        (4, 107, 5.0),
    ]
    ratings_df = spark.createDataFrame(data, ["user_id", "movie_id", "rating"])

als = ALS(
    rank=10,
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="movie_id",
    ratingCol="rating",
    nonnegative=True,
)

model = als.fit(ratings_df)
raw_recommendations = model.recommendForAllUsers(3)

gold_df = raw_recommendations.select(
    col("user_id"),
    transform(col("recommendations"), lambda x: x["movie_id"]).alias(
        "recommended_movies"
    ),
    transform(col("recommendations"), lambda x: x["rating"]).alias("scores"),
)

gold_df.show(5, truncate=False)
