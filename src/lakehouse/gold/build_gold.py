
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# =====================================================
# 1. CONFIGURATION
# =====================================================

BUCKET = os.environ["MINIO_BUCKET"]

SILVER_PATH = f"s3a://{BUCKET}/silver/"
GOLD_PATH = f"s3a://{BUCKET}/gold/"

VIDEO_PATH = SILVER_PATH + "silver_youtube_videos"
CHANNEL_PATH = SILVER_PATH + "silver_channels"
CATEGORY_PATH = SILVER_PATH + "silver_categories"

GOLD_DIM_VIDEO = GOLD_PATH + "dim_video"
GOLD_DIM_CHANNEL = GOLD_PATH + "dim_channel"
GOLD_DIM_CATEGORY = GOLD_PATH + "dim_category"
GOLD_FACT_VIDEO = GOLD_PATH + "fact_video_daily"


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-Build-Gold")
    .master("local[2]")

    # S3A
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["MINIO_ROOT_USER"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["MINIO_ROOT_PASSWORD"]
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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
# 3. READ SILVER TABLES
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    print("\n===== READING SILVER =====")

    silver_video = (
        spark.read.format("delta").load(VIDEO_PATH)
    )

    silver_channel = (
        spark.read.format("delta").load(CHANNEL_PATH)
    )

    silver_category = (
        spark.read.format("delta").load(CATEGORY_PATH)
    )

    video_count = silver_video.count()
    channel_count = silver_channel.count()
    category_count = silver_category.count()

    print("Silver Videos:", video_count)
    print("Silver Channels:", channel_count)
    print("Silver Categories:", category_count)

    # =================================================
    # 4. VALIDATE DIMENSION KEYS
    # =================================================

    print("\n===== VALIDATING DIMENSION KEYS =====")

    def duplicate_count(df, key):
        return (
            df.groupBy(key)
            .count()
            .filter(col("count") > 1)
            .count()
        )

    duplicate_video = duplicate_count(
        silver_video, "video_id"
    )

    duplicate_channel = duplicate_count(
        silver_channel, "channel_id"
    )

    duplicate_category = duplicate_count(
        silver_category, "category_id"
    )

    print("Duplicate video_id:", duplicate_video)
    print("Duplicate channel_id:", duplicate_channel)
    print("Duplicate category_id:", duplicate_category)

    if any([
        duplicate_video,
        duplicate_channel,
        duplicate_category
    ]):
        raise ValueError("Duplicate dimension keys found!")

    # =================================================
    # 5. BUILD DIM_VIDEO
    # =================================================

    print("\n===== BUILDING DIM_VIDEO =====")

    dim_video = silver_video.select(
        "video_id",
        "title",
        "channel_id",
        "published_at"
    )

    # =================================================
    # 6. BUILD DIM_CHANNEL
    # =================================================

    print("\n===== BUILDING DIM_CHANNEL =====")

    dim_channel = silver_channel.select(
        "channel_id",
        "channel_title"
    )

    # =================================================
    # 7. BUILD DIM_CATEGORY
    # =================================================

    print("\n===== BUILDING DIM_CATEGORY =====")

    dim_category = silver_category.select(
        "category_id",
        "category_name"
    )

    # =================================================
    # 8. BUILD FACT_VIDEO_DAILY
    # =================================================

    print("\n===== BUILDING FACT_VIDEO_DAILY =====")

    fact_video = silver_video.select(
        # Foreign keys
        "video_id",
        "channel_id",
        "category_id",

        # Observation time
        "collected_at",

        # Measures
        "views",
        "likes",
        "comments",
        "engagement_rate"
    )

    # =================================================
    # 9. VALIDATE FACT GRAIN
    # =================================================

    print("\n===== VALIDATING FACT GRAIN =====")

    # Logical grain:
    # One video at one collection timestamp.
    #
    # NULL collected_at is retained.
    # Duplicate checking includes NULL timestamps.

    duplicate_fact = (
        fact_video
        .groupBy("video_id", "collected_at")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(
        "Duplicate video_id + collected_at:",
        duplicate_fact
    )

    if duplicate_fact > 0:
        raise ValueError("Duplicate Fact grain found!")

    # =================================================
    # 10. FOREIGN KEY VALIDATION
    # =================================================

    print("\n===== VALIDATING FOREIGN KEYS =====")

    missing_video = (
        fact_video
        .select("video_id")
        .distinct()
        .join(
            dim_video.select("video_id"),
            on="video_id",
            how="left_anti"
        )
        .count()
    )

    missing_channel = (
        fact_video
        .select("channel_id")
        .distinct()
        .join(
            dim_channel.select("channel_id"),
            on="channel_id",
            how="left_anti"
        )
        .count()
    )

    missing_category = (
        fact_video
        .select("category_id")
        .distinct()
        .join(
            dim_category.select("category_id"),
            on="category_id",
            how="left_anti"
        )
        .count()
    )

    print("Missing video references:", missing_video)
    print("Missing channel references:", missing_channel)
    print("Missing category references:", missing_category)

    if any([
        missing_video,
        missing_channel,
        missing_category
    ]):
        raise ValueError("Foreign key validation failed!")

    # =================================================
    # 11. WRITE DIMENSIONS
    # =================================================

    print("\n===== WRITING GOLD DIMENSIONS =====")

    (
        dim_video.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_DIM_VIDEO)
    )

    print("dim_video: SUCCESS")

    (
        dim_channel.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_DIM_CHANNEL)
    )

    print("dim_channel: SUCCESS")

    (
        dim_category.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_DIM_CATEGORY)
    )

    print("dim_category: SUCCESS")

    # =================================================
    # 12. WRITE FACT
    # =================================================

    print("\n===== WRITING GOLD FACT =====")

    (
        fact_video.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_FACT_VIDEO)
    )

    print("fact_video_daily: SUCCESS")

    # =================================================
    # 13. READ BACK AND VALIDATE
    # =================================================

    print("\n===== FINAL GOLD VALIDATION =====")

    result_dim_video = (
        spark.read.format("delta").load(GOLD_DIM_VIDEO)
    )

    result_dim_channel = (
        spark.read.format("delta").load(GOLD_DIM_CHANNEL)
    )

    result_dim_category = (
        spark.read.format("delta").load(GOLD_DIM_CATEGORY)
    )

    result_fact = (
        spark.read.format("delta").load(GOLD_FACT_VIDEO)
    )

    final_video_count = result_dim_video.count()
    final_channel_count = result_dim_channel.count()
    final_category_count = result_dim_category.count()
    final_fact_count = result_fact.count()

    null_collection_time = (
        result_fact
        .filter(col("collected_at").isNull())
        .count()
    )

    print("\nGold record counts:")

    print("dim_video:", final_video_count)
    print("dim_channel:", final_channel_count)
    print("dim_category:", final_category_count)
    print("fact_video_daily:", final_fact_count)

    print(
        "Fact NULL collected_at:",
        null_collection_time
    )

    print("\nFACT SCHEMA:")
    result_fact.printSchema()

    print("\nFACT SAMPLE:")
    result_fact.show(10, truncate=False)

    # =================================================
    # 14. FINAL ASSERTIONS
    # =================================================

    if final_video_count != video_count:
        raise ValueError("dim_video count mismatch!")

    if final_channel_count != channel_count:
        raise ValueError("dim_channel count mismatch!")

    if final_category_count != category_count:
        raise ValueError("dim_category count mismatch!")

    if final_fact_count != video_count:
        raise ValueError("fact_video_daily count mismatch!")

    if null_collection_time != 407:
        raise ValueError("Unexpected NULL collected_at count!")

    print("\n" + "=" * 60)
    print("GOLD STAR SCHEMA PIPELINE: SUCCESS")
    print("=" * 60)

finally:
    spark.stop()
