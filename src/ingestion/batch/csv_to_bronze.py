
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    regexp_extract
)


# =====================================================
# 1. CONFIGURATION
# =====================================================

RAW_DATA_PATH = "/opt/project/data/raw/youtube_master_*.csv"

MINIO_ENDPOINT = "http://minio:9000"

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

BRONZE_PATH = (
    f"s3a://{MINIO_BUCKET}/bronze/youtube_csv_raw"
)

EXPECTED_COLUMNS = [
    "video_id",
    "title",
    "channel_id",
    "channel_title",
    "category_id",
    "category_name",
    "published_at",
    "duration_sec",
    "video_type",
    "views",
    "likes",
    "comments",
    "engagement_rate",
    "top_comments",
    "thumbnail_url",
    "tags",
    "description",
    "subscriber_count",
    "channel_video_count",
    "collected_at",
    "batch_id"
]


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-CSV-To-Bronze")
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

    # Delta Lake
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
# 3. READ CSV FILES
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    print("\nStarting YouTube CSV ingestion...")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")

        # Handle multiline text fields
        .option("multiLine", "true")

        # CSV quotation configuration
        .option("quote", '"')
        .option("escape", '"')

        # Preserve whitespace in source text
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")

        # Do not silently discard malformed records
        .option("mode", "FAILFAST")

        .load(RAW_DATA_PATH)
    )

    # =================================================
    # 4. VALIDATE SOURCE SCHEMA
    # =================================================

    actual_columns = df.columns

    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(
            "CSV schema mismatch!\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual: {actual_columns}"
        )

    print("CSV schema validation: SUCCESS")

    # =================================================
    # 5. ADD SOURCE METADATA
    # =================================================

    bronze_df = (
        df
        .withColumn(
            "source_file",
            regexp_extract(
                input_file_name(),
                r"([^/]+)$",
                1
            )
        )
        .withColumn(
            "source_category",
            regexp_extract(
                input_file_name(),
                r"youtube_master_(.*)\.csv$",
                1
            )
        )
        .withColumn(
            "ingested_at",
            current_timestamp()
        )
    )

    # =================================================
    # 6. VALIDATE RECORD COUNT
    # =================================================

    print("\nCounting source records...")

    total_records = bronze_df.count()

    if total_records == 0:
        raise ValueError("No CSV records found!")

    print("Total source records:", total_records)

    print("\nRecords by category:")

    (
        bronze_df
        .groupBy("source_category")
        .count()
        .orderBy("source_category")
        .show(truncate=False)
    )

    # =================================================
    # 7. WRITE BRONZE DELTA TABLE
    # =================================================

    print("\nWriting Bronze Delta Table...")

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    print("BRONZE CSV WRITE: SUCCESS")

    # =================================================
    # 8. VERIFY BRONZE DATA
    # =================================================

    print("\nVerifying Bronze Delta Table...")

    result = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    saved_records = result.count()

    print("Source records:", total_records)
    print("Saved records:", saved_records)

    if saved_records != total_records:
        raise ValueError("Bronze record count mismatch!")

    print("\nBronze schema:")
    result.printSchema()

    print("\nSample records:")

    result.select(
        "video_id",
        "title",
        "category_name",
        "views",
        "collected_at",
        "source_category",
        "source_file"
    ).show(10, truncate=False)

    print("\n" + "=" * 55)
    print("YOUTUBE CSV BRONZE INGESTION: SUCCESS")
    print("Total records:", saved_records)
    print("Delta location:", BRONZE_PATH)
    print("=" * 55)

finally:
    spark.stop()
