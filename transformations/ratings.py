from spark_config import get_spark_session
from pyspark.sql.functions import col, from_unixtime


def main():
    spark = get_spark_session("Processing_Ratings")

    try:
        df_raw = spark.read.csv(
            "s3a://warehouse/movielens_raw/ratings.csv", header=True, inferSchema=True
        )
        df_silver = df_raw.withColumn(
            "timestamp", from_unixtime(col("timestamp")).cast("timestamp")
        )

        # Making sure the Namespace exist in the Iceberg
        spark.sql("CREATE NAMESPACE IF NOT EXISTS movielens")
        df_silver.write.format("iceberg").mode("overwrite").saveAsTable(
            "my_catalog.movielens.ratings"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
