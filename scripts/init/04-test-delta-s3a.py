
import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


# =====================================================
# 1. S3 / SILO CONFIGURATION
# =====================================================

endpoint = "http://minio:9000"

access_key = os.environ["MINIO_ROOT_USER"]
secret_key = os.environ["MINIO_ROOT_PASSWORD"]
bucket = os.environ["MINIO_BUCKET"]


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-Delta-Test")
    .master("local[1]")

    # S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
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
# 3. TEST DELTA LAKE
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    # Sample data
    data = [
        (1, "Music", 150000, 12000),
        (2, "Gaming", 230000, 18000),
        (3, "Comedy", 180000, 15000),
    ]

    df = spark.createDataFrame(
        data,
        ["video_id", "category", "views", "likes"]
    )

    path = f"s3a://{bucket}/bronze/test_delta"

    # -------------------------------------------------
    # WRITE DELTA TABLE
    # -------------------------------------------------

    print("\nWriting Delta Table...")

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path)

    print("DELTA WRITE: SUCCESS")

    # -------------------------------------------------
    # READ DELTA TABLE
    # -------------------------------------------------

    print("\nReading Delta Table...")

    result = (
        spark.read
        .format("delta")
        .load(path)
    )

    result.show(truncate=False)

    assert result.count() == 3

    print("DELTA READ: SUCCESS")

    # -------------------------------------------------
    # CHECK DELTA TRANSACTION HISTORY
    # -------------------------------------------------

    print("\nDelta Transaction History:")

    delta_table = DeltaTable.forPath(spark, path)

    history = (
        delta_table.history(1)
        .select("version", "operation")
    )

    history.show(truncate=False)

    version = history.first()["version"]
    operation = history.first()["operation"]

    print(f"Delta version: {version}")
    print(f"Operation: {operation}")

    assert version >= 0
    assert operation == "WRITE"

    print("\nDELTA TRANSACTION LOG: SUCCESS")

    # -------------------------------------------------
    # FINAL RESULT
    # -------------------------------------------------

    print("\n" + "=" * 50)
    print("ALL DELTA LAKE TESTS PASSED")
    print("=" * 50)

finally:
    spark.stop()
