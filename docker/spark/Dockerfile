FROM apache/spark:4.0.3-scala2.13-java17-python3-r-ubuntu

USER root

RUN cd /opt/spark/jars && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.11.0/iceberg-aws-1.11.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
    curl -fLO https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.23.19/bundle-2.23.19.jar
