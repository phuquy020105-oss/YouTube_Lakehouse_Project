
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


# =====================================================
# 1. CONFIGURATION
# =====================================================

BUCKET = os.environ["MINIO_BUCKET"]
BASE_PATH = f"s3a://{BUCKET}/gold/"

VIDEO_PATH = BASE_PATH + "dim_video"
CHANNEL_PATH = BASE_PATH + "dim_channel"
CATEGORY_PATH = BASE_PATH + "dim_category"
FACT_PATH = BASE_PATH + "fact_video_daily"


# =====================================================
# 2. SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTube-Gold-Validation")
    .master("local[2]")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",
            os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key",
            os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

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


try:
    spark.sparkContext.setLogLevel("WARN")

    # =================================================
    # 3. READ GOLD TABLES
    # =================================================

    print("\n===== READING GOLD TABLES =====")

    video = spark.read.format("delta").load(VIDEO_PATH)
    channel = spark.read.format("delta").load(CHANNEL_PATH)
    category = spark.read.format("delta").load(CATEGORY_PATH)
    fact = spark.read.format("delta").load(FACT_PATH)

    # =================================================
    # 4. TABLE COUNTS
    # =================================================

    print("\n===== GOLD TABLE COUNTS =====")

    video_count = video.count()
    channel_count = channel.count()
    category_count = category.count()
    fact_count = fact.count()

    print("dim_video:", video_count)
    print("dim_channel:", channel_count)
    print("dim_category:", category_count)
    print("fact_video_daily:", fact_count)

    # =================================================
    # 5. PRIMARY KEY VALIDATION
    # =================================================

    print("\n===== PRIMARY KEY VALIDATION =====")

    def duplicate_count(df, key):
        return (
            df.groupBy(key)
            .count()
            .filter(col("count") > 1)
            .count()
        )

    duplicate_video = duplicate_count(video, "video_id")
    duplicate_channel = duplicate_count(channel, "channel_id")
    duplicate_category = duplicate_count(category, "category_id")

    duplicate_fact = (
        fact.groupBy("video_id", "collected_at")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("Duplicate video_id:", duplicate_video)
    print("Duplicate channel_id:", duplicate_channel)
    print("Duplicate category_id:", duplicate_category)
    print("Duplicate Fact grain:", duplicate_fact)

    # =================================================
    # 6. NULL KEY VALIDATION
    # =================================================

    print("\n===== NULL KEY VALIDATION =====")

    null_fact_video = fact.filter(
        col("video_id").isNull()
    ).count()

    null_fact_channel = fact.filter(
        col("channel_id").isNull()
    ).count()

    null_fact_category = fact.filter(
        col("category_id").isNull()
    ).count()

    null_video_channel = video.filter(
        col("channel_id").isNull()
    ).count()

    print("NULL fact video_id:", null_fact_video)
    print("NULL fact channel_id:", null_fact_channel)
    print("NULL fact category_id:", null_fact_category)
    print("NULL dim_video channel_id:", null_video_channel)

    # =================================================
    # 7. FOREIGN KEY VALIDATION
    # =================================================

    print("\n===== FOREIGN KEY VALIDATION =====")

    missing_video = (
        fact.select("video_id").distinct()
        .join(
            video.select("video_id"),
            "video_id",
            "left_anti"
        )
        .count()
    )

    missing_channel = (
        fact.select("channel_id").distinct()
        .join(
            channel.select("channel_id"),
            "channel_id",
            "left_anti"
        )
        .count()
    )

    missing_category = (
        fact.select("category_id").distinct()
        .join(
            category.select("category_id"),
            "category_id",
            "left_anti"
        )
        .count()
    )

    print("Missing video:", missing_video)
    print("Missing channel:", missing_channel)
    print("Missing category:", missing_category)

    # =================================================
    # 8. NULL COLLECTION TIME
    # =================================================

    print("\n===== TIMESTAMP VALIDATION =====")

    null_collection_time = (
        fact.filter(col("collected_at").isNull()).count()
    )

    print("NULL collected_at:", null_collection_time)

    # =================================================
    # 9. CATEGORY ANALYTICS
    # =================================================

    print("\n===== CATEGORY ANALYTICS =====")

    category_metrics = (
        fact.join(category, "category_id", "left")
        .groupBy("category_name")
        .agg(
            spark_sum("views").alias("total_views"),
            spark_sum("likes").alias("total_likes"),
            spark_sum("comments").alias("total_comments")
        )
        .orderBy("category_name")
    )

    category_metrics.show(truncate=False)

    # =================================================
    # 10. FINAL VALIDATION
    # =================================================

    print("\n===== FINAL VALIDATION =====")

    errors = []

    if video_count != 36352:
        errors.append("dim_video count mismatch")

    if channel_count != 6255:
        errors.append("dim_channel count mismatch")

    if category_count != 4:
        errors.append("dim_category count mismatch")

    if fact_count != 36352:
        errors.append("fact_video_daily count mismatch")

    if duplicate_video or duplicate_channel or duplicate_category:
        errors.append("Duplicate dimension keys")

    if duplicate_fact:
        errors.append("Duplicate Fact grain")

    if null_fact_video or null_fact_channel or null_fact_category:
        errors.append("NULL Fact foreign keys")

    if null_video_channel:
        errors.append("NULL channel_id in dim_video")

    if missing_video or missing_channel or missing_category:
        errors.append("Foreign key relationship failure")

    if null_collection_time != 407:
        errors.append("Unexpected NULL collected_at count")

    if errors:
        print("\nGOLD VALIDATION: FAILED")

        for error in errors:
            print("ERROR:", error)

        raise ValueError("Gold validation failed")

    print("\n" + "=" * 55)
    print("GOLD VALIDATION: SUCCESS")
    print("All four Gold tables passed validation.")
    print("=" * 55)

finally:
    spark.stop()
