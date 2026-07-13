from spark_config import get_spark_session

spark = get_spark_session("Inspection")

df_movies = spark.read.table("movielens.silver.movies")
df_ratings = spark.read.table("movielens.silver.ratings")
df_tags = spark.read.table("movielens.silver.tags")
df_links = spark.read.table("movielens.silver.links")
df_rec = spark.read.table("movielens.gold.als_recommendations")
