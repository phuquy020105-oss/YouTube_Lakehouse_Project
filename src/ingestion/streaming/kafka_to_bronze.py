
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# =====================================================
# 1. CONFIGURATION
# =====================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

KAFKA_TOPIC = "youtube.public.youtube_cdc_test"

MINIO_ENDPOINT = "http://minio:9000"

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

BRONZE_PATH = (
    f"s3a://{MINIO_BUCKET}/bronze/youtube_cdc_events"
)

CHECKPOINT_PATH = (
    "/opt/project/data/checkpoints/youtube_cdc_bronze"
)


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-KafkaToBronze")
    .master("local[2]")

    # S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    # Delta Lake configuration
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    .getOrCreate()
)


# =====================================================
# 3. READ DATA FROM KAFKA
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    print("\nConnecting to Kafka...")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    # =================================================
    # 4. PRESERVE RAW CDC EVENTS
    # =================================================

    bronze_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at")
    )

    # =================================================
    # 5. WRITE TO BRONZE DELTA LAKE
    # =================================================

    print("\nStarting Kafka -> Bronze Delta Lake...")

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )

    query.awaitTermination()

    print("\nBRONZE INGESTION: SUCCESS")
    print("Kafka Topic:", KAFKA_TOPIC)
    print("Delta Location:", BRONZE_PATH)

finally:
    spark.stop()
