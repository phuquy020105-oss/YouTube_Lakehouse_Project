
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# =====================================================
# 1. PROJECT CONFIGURATION
# =====================================================

PROJECT_ROOT = r"D:\YouTube_Lakehouse"

SPARK_IMAGE = "youtube-lakehouse-spark:3.5.3"

DOCKER_URL = "unix://var/run/docker.sock"

DOCKER_NETWORK = "youtube_lakehouse_default"


# =====================================================
# 2. MINIO ENVIRONMENT
# Read directly from Airflow Scheduler environment
# =====================================================

MINIO_ENV = {
    "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
    "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
    "MINIO_BUCKET": os.environ["MINIO_BUCKET"],
}


# =====================================================
# 3. DOCKER OPERATOR FACTORY
# =====================================================

def create_spark_task(
    task_id,
    script,
    data_read_only=True,
    timeout=7200
):

    mounts = [
        Mount(
            source=PROJECT_ROOT + r"\src",
            target="/opt/project/src",
            type="bind",
            read_only=True
        ),

        Mount(
            source=PROJECT_ROOT + r"\data",
            target="/opt/project/data",
            type="bind",
            read_only=data_read_only
        )
    ]

    return DockerOperator(
        task_id=task_id,

        image=SPARK_IMAGE,

        command=[
            "python",
            f"/opt/project/src/{script}"
        ],

        docker_url=DOCKER_URL,

        network_mode=DOCKER_NETWORK,

        mounts=mounts,

        environment=MINIO_ENV,

        mount_tmp_dir=False,

        auto_remove="success",

        force_pull=False,

        do_xcom_push=False,

        tty=False,

        execution_timeout=timedelta(
            seconds=timeout
        )
    )


# =====================================================
# 4. DEFAULT ARGUMENTS
# =====================================================

default_args = {
    "owner": "youtube-lakehouse",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(minutes=3),
}


# =====================================================
# 5. DAG DEFINITION
# =====================================================

with DAG(

    dag_id="youtube_lakehouse_pipeline",

    description=(
        "YouTube Lakehouse Bronze Silver Gold "
        "orchestration using Spark and Docker"
    ),

    default_args=default_args,

    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),

    # Manual execution during testing
    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "youtube",
        "lakehouse",
        "spark",
        "bronze",
        "silver",
        "gold"
    ],

) as dag:

    # =================================================
    # 6. BRONZE CSV
    # =================================================

    bronze_csv = create_spark_task(
        task_id="bronze_csv_ingestion",

        script="ingestion/batch/csv_to_bronze.py",

        data_read_only=True,

        timeout=3600
    )

    # =================================================
    # 7. SILVER CSV
    # =================================================

    silver_csv = create_spark_task(
        task_id="silver_csv_processing",

        script="lakehouse/silver/build_silver_csv.py",

        data_read_only=True,

        timeout=3600
    )

    # =================================================
    # 8. GOLD STAR SCHEMA
    # =================================================

    gold = create_spark_task(
        task_id="gold_star_schema",

        script="lakehouse/gold/build_gold.py",

        data_read_only=True,

        timeout=3600
    )

    # =================================================
    # 9. BRONZE CDC
    # =================================================

    bronze_cdc = create_spark_task(
        task_id="bronze_cdc_ingestion",

        script="ingestion/streaming/kafka_to_bronze.py",

        # Checkpoint needs write access
        data_read_only=False,

        timeout=3600
    )

    # =================================================
    # 10. SILVER CDC
    # =================================================

    silver_cdc = create_spark_task(
        task_id="silver_cdc_processing",

        script="lakehouse/silver/silver_cdc.py",

        data_read_only=True,

        timeout=3600
    )

    # =================================================
    # 11. TASK DEPENDENCIES
    # =================================================

    # CSV pipeline
    bronze_csv >> silver_csv >> gold

    # CDC pipeline
    bronze_cdc >> silver_cdc
