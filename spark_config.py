from pyspark.sql import SparkSession
import os

REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")


def get_spark_session(app_name: str = "MovieLens_Pipeline") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.11.0,"
            "org.apache.hadoop:hadoop-aws:3.4.0,"
            "software.amazon.awssdk:bundle:2.24.6",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Conexão com o Iceberg REST e definindo o catálogo 'movielens'
        .config(
            "spark.sql.catalog.movielens",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.movielens.type", "rest")
        .config("spark.sql.catalog.movielens.uri", REST_URI)  # rest gate
        .config(
            "spark.sql.catalog.movielens.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",  # Official AWS client
        )  # use s3 protocol
        # Conexão com o MinIO
        .config("spark.sql.catalog.movielens.s3.endpoint", S3_ENDPOINT)
        .config(
            "spark.sql.catalog.movielens.s3.path-style-access", "true"
        )  # Acesso apenas local
        # Hadoop Config #############################################################
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Configuring Hadoop credentials
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )  # Comando para o Hadoop apenas ler as linhas abaixo
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        # Configurando "movielens" como padrão
        .config("spark.sql.defaultCatalog", "movielens")
        # Definindo Systems Properties para o AWS SDK do Java, para comunicar-se com o Iceberg
        .config(
            "spark.driver.extraJavaOptions",
            "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
        )
        # Ajustes no uso da memória
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
