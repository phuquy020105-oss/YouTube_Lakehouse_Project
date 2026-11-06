
import os
import ntpath
import posixpath
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# =====================================================
# 1. PROJECT CONFIGURATION
# =====================================================

PROJECT_ROOT = os.environ["PROJECT_ROOT"]

SPARK_IMAGE = "youtube-lakehouse-spark:3.5.3"

DOCKER_URL = "unix://var/run/docker.sock"

DOCKER_NETWORK = "youtube_lakehouse_default"


# =====================================================
# 2. PORTABLE HOST PATH
# WINDOWS / MACOS / LINUX
# =====================================================


def get_host_path(directory):

    # Windows
    if len(PROJECT_ROOT) >= 2 and PROJECT_ROOT[1] == ":":
        return ntpath.join(
            ntpath.normpath(PROJECT_ROOT),
            directory
        )

    # macOS / Linux
    return posixpath.normpath(
        posixpath.join(PROJECT_ROOT, directory)
    )

SRC_PATH = get_host_path("src")

DATA_PATH = get_host_path("data")


# =====================================================
# 3. MINIO ENVIRONMENT
# =====================================================

MINIO_ENV = {
    "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
    "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
    "MINIO_BUCKET": os.environ["MINIO_BUCKET"],
}


# =====================================================
# 4. DOCKER OPERATOR FACTORY
# =====================================================

def create_spark_task(
    task_id,
    script,
    data_read_only=True,
    timeout=7200
):

    mounts = [

        Mount(
            source=SRC_PATH,
            target="/opt/project/src",
            type="bind",
            read_only=True
        ),

        Mount(
            source=DATA_PATH,
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
# 5. DEFAULT ARGUMENTS
# =====================================================

default_args = {

    "owner": "youtube-lakehouse",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(minutes=3),

}


# =====================================================
# 6. DAG DEFINITION
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
    # 7. BRONZE CSV
    # =================================================

    bronze_csv = create_spark_task(

        task_id="bronze_csv_ingestion",

        script="ingestion/batch/csv_to_bronze.py",

        data_read_only=True,

        timeout=3600

    )

    # =================================================
    # 8. SILVER CSV
    # =================================================

    silver_csv = create_spark_task(

        task_id="silver_csv_processing",

        script="lakehouse/silver/build_silver_csv.py",

        data_read_only=True,

        timeout=3600

    )

    # =================================================
    # 9. GOLD STAR SCHEMA
    # =================================================

    gold = create_spark_task(

        task_id="gold_star_schema",

        script="lakehouse/gold/build_gold.py",

        data_read_only=True,

        timeout=3600

    )

    # =================================================
    # 10. BRONZE CDC
    # =================================================

    bronze_cdc = create_spark_task(

        task_id="bronze_cdc_ingestion",

        script="ingestion/streaming/kafka_to_bronze.py",

        data_read_only=False,

        timeout=3600

    )

    # =================================================
    # 11. SILVER CDC
    # =================================================

    silver_cdc = create_spark_task(

        task_id="silver_cdc_processing",

        script="lakehouse/silver/silver_cdc.py",

        data_read_only=True,

        timeout=3600

    )

    # =================================================
    # 12. TASK DEPENDENCIES
    # =================================================

    bronze_csv >> silver_csv >> gold

    bronze_cdc >> silver_cdc
