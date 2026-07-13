from spark_config import get_spark_session
from pyspark.sql.functions import col, regexp_extract, split, regexp_replace


def main():
    spark = get_spark_session("Processing_Movies")

    try:
        df_raw = spark.read.csv(
            "s3a://warehouse/movielens_raw/movies.csv", header=True, inferSchema=True
        )
        df_silver = (
            df_raw.withColumn("year", regexp_extract(col("title"), r"\((\d{4})\)", 1))
            .withColumn("year", col("year").try_cast("integer"))
            .withColumn("title", regexp_replace(col("title"), r"\s*\(\d{4}\)\s*", ""))
            .withColumn("genres", split(col("genres"), r"\|"))
        )

        # Making sure the Namespace exist in the Iceberg
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
        df_silver.write.format("iceberg").mode("overwrite").saveAsTable(
            "movielens.silver.movies"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
