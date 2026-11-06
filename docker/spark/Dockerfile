
FROM python:3.11-slim-bookworm

# =====================================================
# JAVA 17 AND SYSTEM UTILITIES
# =====================================================

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# =====================================================
# PYTHON CONFIGURATION
# =====================================================

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/project

# =====================================================
# PYTHON DEPENDENCIES
# =====================================================

COPY docker/spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# =====================================================
# SPARK JAR DIRECTORY
# =====================================================

RUN mkdir -p /opt/project/src \
    /opt/project/data \
    /opt/project/reports

# =====================================================
# HADOOP S3A CONNECTOR
# Hadoop 3.3.4
# =====================================================

ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN SPARK_JARS="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
       -o "${SPARK_JARS}/hadoop-aws-${HADOOP_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
       -o "${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# =====================================================
# DELTA LAKE JVM LIBRARIES
# Spark 3.5.3 - Scala 2.12 - Delta 3.3.0
# =====================================================

ARG DELTA_VERSION=3.3.0

RUN SPARK_JARS="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
       -o "${SPARK_JARS}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
       -o "${SPARK_JARS}/delta-storage-${DELTA_VERSION}.jar"

# =====================================================
# SPARK STRUCTURED STREAMING - KAFKA CONNECTOR
# Spark 3.5.3 - Scala 2.12
# =====================================================

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENT_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
ARG JSR305_VERSION=3.0.0

RUN SPARK_JARS="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
       -o "${SPARK_JARS}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
       -o "${SPARK_JARS}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
       -o "${SPARK_JARS}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
       -o "${SPARK_JARS}/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
    && curl -fL --retry 3 \
       "https://repo.maven.apache.org/maven2/com/google/code/findbugs/jsr305/${JSR305_VERSION}/jsr305-${JSR305_VERSION}.jar" \
       -o "${SPARK_JARS}/jsr305-${JSR305_VERSION}.jar"

# =====================================================
# FINAL
# =====================================================

CMD ["python", "--version"]
