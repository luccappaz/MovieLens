FROM apache/airflow:3.3.0-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==6.1.0 \
    "pyspark==4.0.3" \
    "pandas<2.2.0"

RUN cd /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.11.0/iceberg-aws-1.11.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.23.19/bundle-2.23.19.jar
