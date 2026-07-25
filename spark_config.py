import os

from pyspark.sql import SparkSession

REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "local[*]")


def get_spark_session(app_name: str = "MovieLens_Pipeline") -> SparkSession:

    is_local = "localhost" in S3_ENDPOINT or "localhost" in REST_URI

    print(f"🌍 Ambiente detectado: {'LOCAL (PC)' if is_local else 'DOCKER'}")
    print(f"🔗 Conectando ao MinIO em: {S3_ENDPOINT}")
    print(f"🔗 Conectando ao Catálogo em: {REST_URI}")

    builder = SparkSession.builder.appName(app_name)

    # ====================================================================
    # RESOLUÇÃO DO "JAR HELL"
    # Só injetamos pacotes via internet se estivermos a rodar localmente.
    # No Docker, usamos os JARs nativos da imagem customizada!
    # ====================================================================
    if is_local:
        packages = [
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
            # Usamos a versão NÃO-bundle do Iceberg para evitar conflitos
            # com o Logger do hadoop-aws no momento do desligamento (ShutdownHook)
            "org.apache.iceberg:iceberg-aws:1.11.0",
            "org.apache.hadoop:hadoop-aws:3.4.0",
            "org.slf4j:slf4j-simple:1.7.36",
        ]
        builder = builder.config("spark.jars.packages", ",".join(packages))

    # Construção principal da sessão (Usando o seu catálogo "movielens")
    spark = (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Conexão com o Iceberg REST e definindo o catálogo 'movielens'
        .config("spark.sql.catalog.movielens", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.movielens.type", "rest")
        .config("spark.sql.catalog.movielens.uri", REST_URI)
        .config(
            "spark.sql.catalog.movielens.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        # Conexão com o MinIO
        .config("spark.sql.catalog.movielens.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.movielens.s3.path-style-access", "true")
        # Hadoop Config (Para leitura S3A pura)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
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
        # Definindo o MASTER
        .config("spark.master", SPARK_MASTER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
