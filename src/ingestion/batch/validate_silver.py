
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    when
)


# =====================================================
# 1. CONFIGURATION
# =====================================================

BUCKET = os.environ["MINIO_BUCKET"]

BASE_PATH = f"s3a://{BUCKET}/silver/"

VIDEO_PATH = BASE_PATH + "silver_youtube_videos"
CHANNEL_PATH = BASE_PATH + "silver_channels"
CATEGORY_PATH = BASE_PATH + "silver_categories"


# =====================================================
# 2. SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTube-Silver-Validation")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)


try:
    spark.sparkContext.setLogLevel("WARN")

    videos = spark.read.format("delta").load(VIDEO_PATH)
    channels = spark.read.format("delta").load(CHANNEL_PATH)
    categories = spark.read.format("delta").load(CATEGORY_PATH)

    print("\n===== 1. TABLE COUNTS =====")

    print("Videos:", videos.count())
    print("Channels:", channels.count())
    print("Categories:", categories.count())

    # =================================================
    # 2. CHANNEL CONSISTENCY
    # =================================================

    print("\n===== 2. CHANNEL CONSISTENCY =====")

    channel_conflicts = (
        videos
        .groupBy("channel_id")
        .agg(
            countDistinct("channel_title").alias("name_count")
        )
        .filter(col("name_count") > 1)
    )

    print(
        "Channel ID with multiple names:",
        channel_conflicts.count()
    )

    channel_conflicts.show(10, truncate=False)

    # =================================================
    # 3. CATEGORY CONSISTENCY
    # =================================================

    print("\n===== 3. CATEGORY CONSISTENCY =====")

    category_conflicts = (
        videos
        .groupBy("category_id")
        .agg(
            countDistinct("category_name").alias("name_count")
        )
        .filter(col("name_count") > 1)
    )

    print(
        "Category ID with multiple names:",
        category_conflicts.count()
    )

    category_conflicts.show(10, truncate=False)

    # =================================================
    # 4. ZERO VALUES
    # =================================================

    print("\n===== 4. ZERO VALUES =====")

    for column_name in ["views", "likes", "comments"]:

        zero_count = (
            videos
            .filter(col(column_name) == 0)
            .count()
        )

        print(f"{column_name} = 0:", zero_count)

    # =================================================
    # 5. ENGAGEMENT RATE
    # =================================================

    print("\n===== 5. ENGAGEMENT RATE =====")

    videos.select(
        spark_min("engagement_rate").alias("min_rate"),
        spark_max("engagement_rate").alias("max_rate"),
        spark_sum(
            when(col("engagement_rate").isNull(), 1).otherwise(0)
        ).alias("null_rate")
    ).show()

    # =================================================
    # 6. TIMESTAMP QUALITY
    # =================================================

    print("\n===== 6. TIMESTAMP QUALITY =====")

    videos.select(
        spark_sum(
            when(col("published_at").isNull(), 1).otherwise(0)
        ).alias("null_published_at"),
        spark_sum(
            when(col("collected_at").isNull(), 1).otherwise(0)
        ).alias("null_collected_at")
    ).show()

    # =================================================
    # 7. FOREIGN KEY CHECK
    # =================================================

    print("\n===== 7. FOREIGN KEY CHECK =====")

    missing_channels = (
        videos
        .select("channel_id")
        .distinct()
        .join(
            channels.select("channel_id"),
            on="channel_id",
            how="left_anti"
        )
        .count()
    )

    missing_categories = (
        videos
        .select("category_id")
        .distinct()
        .join(
            categories.select("category_id"),
            on="category_id",
            how="left_anti"
        )
        .count()
    )

    print("Missing channel references:", missing_channels)
    print("Missing category references:", missing_categories)

    print("\nSILVER VALIDATION COMPLETED")

finally:
    spark.stop()
