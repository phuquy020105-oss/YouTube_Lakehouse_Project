
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

ENV AIRFLOW_HOME=/opt/airflow

# Install Airflow providers
RUN pip install --no-cache-dir \
    "apache-airflow[apache-spark,postgres,docker]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Align PySpark with project Spark version
RUN pip install --no-cache-dir --no-deps pyspark==3.5.3

WORKDIR /opt/airflow

RUN mkdir -p \
    /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins

USER airflow

CMD ["airflow", "version"]
