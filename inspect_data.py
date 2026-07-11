from spark_config import get_spark_session
from pyspark.sql.functions import col, sum, when

spark = get_spark_session("Inspect_Data")

# print("Reading raw data from 'movies.csv' at MinIO...")
# df = spark.read.csv(
#     "s3a://warehouse/movielens_raw/movies.csv", header=True, inferSchema=True
# )

# print("Reading raw data from 'ratings.csv' at MinIO...")
# df = spark.read.csv(
#     "s3a://warehouse/movielens_raw/ratings.csv", header=True, inferSchema=True
# )

# print("Reading raw data from 'links.csv' at MinIO...")
# df = spark.read.csv(
#     "s3a://warehouse/movielens_raw/links.csv", header=True, inferSchema=True
# )

print("Reading raw data from 'tags.csv' at MinIO...")
df = spark.read.csv(
    "s3a://warehouse/movielens_raw/tags.csv", header=True, inferSchema=True
)

print("\n COLUMN SCTRUCTURE (SCHEMA):")
df.printSchema()

print("\n DATA FRAME HEAD")
df.show(5, truncate=False)

print("\n SHOWING NULL DATA INFO")
null_values = df.select(
    [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)
null_values.show()

print("\n SHOWING DUPLICATA INFO")
df_duplicatas = df.groupby("userId", "movieId").count().filter(col("count") > 1)
if df_duplicatas.isEmpty():
    print("No duplicatas")
else:
    print("There is duplicatas in the dataframe")
