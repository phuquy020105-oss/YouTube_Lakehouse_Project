
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    when,
    first,
    abs as spark_abs
)


# =====================================================
# 1. CONFIGURATION
# =====================================================

MINIO_ENDPOINT = "http://minio:9000"

BUCKET = os.environ["MINIO_BUCKET"]

BRONZE_PATH = (
    f"s3a://{BUCKET}/bronze/youtube_csv_raw"
)

SILVER_VIDEO_PATH = (
    f"s3a://{BUCKET}/silver/silver_youtube_videos"
)

SILVER_CHANNEL_PATH = (
    f"s3a://{BUCKET}/silver/silver_channels"
)

SILVER_CATEGORY_PATH = (
    f"s3a://{BUCKET}/silver/silver_categories"
)


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-Build-Silver-CSV")
    .master("local[2]")

    # S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
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
# 3. READ BRONZE
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    print("\n===== READING BRONZE =====")

    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    bronze_count = bronze_df.count()

    print("Bronze records:", bronze_count)

    # =================================================
    # 4. DATA QUALITY VALIDATION
    # =================================================

    print("\n===== VALIDATING BRONZE =====")

    duplicate_count = (
        bronze_df
        .groupBy("video_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_video_count = (
        bronze_df
        .filter(
            col("video_id").isNull()
            | (col("video_id") == "")
        )
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate video_id groups"
        )

    if null_video_count > 0:
        raise ValueError(
            f"Found {null_video_count} NULL video_id records"
        )

    print("Duplicate video_id:", duplicate_count)
    print("NULL video_id:", null_video_count)

    # =================================================
    # 5. TRANSFORM SILVER VIDEO
    # =================================================

    print("\n===== BUILDING SILVER VIDEOS =====")

    silver_video = (
        bronze_df

        # Integer columns
        .withColumn(
            "duration_sec",
            col("duration_sec").cast("integer")
        )
        .withColumn(
            "category_id",
            col("category_id").cast("integer")
        )
        .withColumn(
            "views",
            col("views").cast("long")
        )
        .withColumn(
            "likes",
            col("likes").cast("long")
        )
        .withColumn(
            "comments",
            col("comments").cast("long")
        )
        .withColumn(
            "subscriber_count",
            col("subscriber_count").cast("long")
        )
        .withColumn(
            "channel_video_count",
            col("channel_video_count").cast("long")
        )

        # Floating point
        .withColumn(
            "engagement_rate",
            col("engagement_rate").cast("double")
        )

        # Timestamp conversion
        .withColumn(
            "published_at",
            to_timestamp(
                col("published_at"),
                "yyyy-MM-dd'T'HH:mm:ssX"
            )
        )
        .withColumn(
            "collected_at",
            to_timestamp(
                col("collected_at"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )

        # =================================================
        # DATA QUALITY FLAGS
        # =================================================

        # Missing collection timestamp
        .withColumn(
            "is_collected_at_missing",
            col("collected_at").isNull()
        )

        # Recalculated engagement rate
        .withColumn(
            "calculated_engagement_rate",
            when(
                col("views") > 0,
                (col("likes") + col("comments"))
                / col("views") * 100
            )
        )

        # Likes greater than views
        .withColumn(
            "is_likes_gt_views",
            when(
                col("views").isNotNull()
                & col("likes").isNotNull(),
                col("likes") > col("views")
            ).otherwise(False)
        )

        # Comments greater than views
        .withColumn(
            "is_comments_gt_views",
            when(
                col("views").isNotNull()
                & col("comments").isNotNull(),
                col("comments") > col("views")
            ).otherwise(False)
        )

        # Engagement rate greater than 100%
        .withColumn(
            "is_engagement_rate_gt_100",
            when(
                col("engagement_rate").isNotNull(),
                col("engagement_rate") > 100
            ).otherwise(False)
        )

        # Zero views but has interactions
        .withColumn(
            "is_zero_views_with_interactions",
            when(
                col("views") == 0,
                (col("likes") > 0)
                | (col("comments") > 0)
            ).otherwise(False)
        )

        # Formula mismatch
        .withColumn(
            "is_engagement_rate_mismatch",
            when(
                col("views") > 0,
                spark_abs(
                    col("engagement_rate")
                    - col("calculated_engagement_rate")
                ) > 0.02
            ).otherwise(False)
        )
    )

    # =================================================
    # 6. VALIDATE NUMERIC CONVERSION
    # =================================================

    print("\n===== CHECKING NUMERIC CONVERSION =====")

    numeric_columns = [
        "duration_sec",
        "category_id",
        "views",
        "likes",
        "comments",
        "subscriber_count",
        "channel_video_count",
        "engagement_rate"
    ]

    for column_name in numeric_columns:

        invalid_count = (
            silver_video
            .filter(
                col(column_name).isNull()
                & (
                    bronze_df[column_name].isNotNull()
                    & (bronze_df[column_name] != "")
                )
            )
            .count()
        )

        print(f"{column_name}: invalid={invalid_count}")

        if invalid_count > 0:
            raise ValueError(
                f"Invalid numeric conversion: {column_name}"
            )

    # =================================================
    # 7. WRITE SILVER VIDEO
    # =================================================

    print("\n===== WRITING SILVER VIDEOS =====")

    (
        silver_video.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_VIDEO_PATH)
    )

    print("Silver Videos: SUCCESS")

    # =================================================
    # 8. BUILD SILVER CHANNELS
    # =================================================

    print("\n===== BUILDING SILVER CHANNELS =====")

    silver_channels = (
        silver_video
        .groupBy("channel_id")
        .agg(
            first(
                "channel_title",
                ignorenulls=True
            ).alias("channel_title")
        )
    )

    (
        silver_channels.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_CHANNEL_PATH)
    )

    print("Silver Channels: SUCCESS")

    # =================================================
    # 9. BUILD SILVER CATEGORIES
    # =================================================

    print("\n===== BUILDING SILVER CATEGORIES =====")

    silver_categories = (
        silver_video
        .groupBy("category_id")
        .agg(
            first(
                "category_name",
                ignorenulls=True
            ).alias("category_name")
        )
    )

    (
        silver_categories.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_CATEGORY_PATH)
    )

    print("Silver Categories: SUCCESS")

    # =================================================
    # 10. FINAL VALIDATION
    # =================================================

    print("\n===== FINAL SILVER VALIDATION =====")

    video_result = (
        spark.read
        .format("delta")
        .load(SILVER_VIDEO_PATH)
    )

    channel_result = (
        spark.read
        .format("delta")
        .load(SILVER_CHANNEL_PATH)
    )

    category_result = (
        spark.read
        .format("delta")
        .load(SILVER_CATEGORY_PATH)
    )

    video_count = video_result.count()
    channel_count = channel_result.count()
    category_count = category_result.count()

    missing_collected = (
        video_result
        .filter(col("is_collected_at_missing"))
        .count()
    )

    likes_gt_views = (
        video_result
        .filter(col("is_likes_gt_views"))
        .count()
    )

    comments_gt_views = (
        video_result
        .filter(col("is_comments_gt_views"))
        .count()
    )

    engagement_gt_100 = (
        video_result
        .filter(col("is_engagement_rate_gt_100"))
        .count()
    )

    zero_views_interactions = (
        video_result
        .filter(col("is_zero_views_with_interactions"))
        .count()
    )

    formula_mismatch = (
        video_result
        .filter(col("is_engagement_rate_mismatch"))
        .count()
    )

    # =================================================
    # 11. REPORT
    # =================================================

    print("\n" + "=" * 55)
    print("SILVER DATA QUALITY REPORT")
    print("=" * 55)

    print("Bronze videos:", bronze_count)
    print("Silver videos:", video_count)
    print("Silver channels:", channel_count)
    print("Silver categories:", category_count)

    print("\nData quality flags:")

    print("Missing collected_at:", missing_collected)
    print("Likes > Views:", likes_gt_views)
    print("Comments > Views:", comments_gt_views)
    print("Engagement > 100%:", engagement_gt_100)
    print("Zero views with interactions:", zero_views_interactions)
    print("Engagement formula mismatch:", formula_mismatch)

    if video_count != bronze_count:
        raise ValueError("Silver video count mismatch!")

    if channel_count != 6255:
        raise ValueError("Unexpected channel count!")

    if category_count != 4:
        raise ValueError("Unexpected category count!")

    if missing_collected != 407:
        raise ValueError("Unexpected missing collected_at count!")

    print("\nSample Silver Videos:")

    video_result.select(
        "video_id",
        "views",
        "likes",
        "comments",
        "engagement_rate",
        "calculated_engagement_rate",
        "is_likes_gt_views",
        "is_zero_views_with_interactions"
    ).show(10, truncate=False)

    print("\n" + "=" * 55)
    print("SILVER CSV PIPELINE: SUCCESS")
    print("All validation checks passed.")
    print("=" * 55)

finally:
    spark.stop()
