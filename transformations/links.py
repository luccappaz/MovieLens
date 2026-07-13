from spark_config import get_spark_session


def main():
    spark = get_spark_session("Processing_Links")

    try:
        df_raw = spark.read.csv(
            "s3a://warehouse/movielens_raw/links.csv", header=True, inferSchema=True
        )

        df_silver = df_raw
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

        df_silver.write.format("iceberg").mode("overwrite").saveAsTable(
            "movielens.silver.links"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
