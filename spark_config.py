from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "MovieLens_Pipeline") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Conexão com o Iceberg REST e definindo o catálogo (the 'database')
        .config(
            "spark.sql.catalog.my_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.my_catalog.type", "rest")
        .config(
            "spark.sql.catalog.my_catalog.uri", "http://localhost:8181"
        )  # rest gate
        .config(
            "spark.sql.catalog.my_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",  # Official AWS client
        )  # use s3 protocol
        # Conexão com o MinIO
        .config("spark.sql.catalog.my_catalog.s3.endpoint", "http://localhost:9000")
        .config(
            "spark.sql.catalog.my_catalog.s3.path-style-access", "true"
        )  # Acesso apenas local
        # Hadoop Config #############################################################
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Configuring Hadoop credentials
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )  # Comando para o Hadoop apenas ler as linhas abaixo
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        # Configurando "my_catalog" como padrão
        .config("spark.sql.defaultCatalog", "my_catalog")
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
    return spark
