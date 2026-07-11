from spark_config import get_spark_session
from pyspark.sql.functions import col, from_unixtime


def main():
    spark = get_spark_session("Processing_Tags")
    df_raw = spark.read.csv(
        "s3a://warehouse/movielens_raw/tags.csv",
        header=True,
        multiLine=True,
    )

    df_silver = (
        df_raw.withColumn("userId", col("userId").cast("integer"))
        .withColumn("movieId", col("movieId").cast("integer"))
        .withColumn(
            "timestamp",
            from_unixtime(col("timestamp").try_cast("bigint")).cast("timestamp"),
        )
        .dropna()
    )

    # Making sure the Namespace exist in the Iceberg
    spark.sql("CREATE NAMESPACE IF NOT EXISTS movielens")

    df_silver.write.format("iceberg").mode("overwrite").saveAsTable(
        "my_catalog.movielens.tags"
    )


if __name__ == "__main__":
    main()
