
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round,
    abs as spark_abs,
    count,
    sum as spark_sum
)

spark = (
    SparkSession.builder
    .appName("YouTube-Engagement-Validation")
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

    path = (
        f"s3a://{os.environ['MINIO_BUCKET']}"
        "/silver/silver_youtube_videos"
    )

    df = spark.read.format("delta").load(path)

    # Recalculate engagement rate
    df = df.withColumn(
        "calculated_engagement_rate",
        when(
            col("views") > 0,
            (col("likes") + col("comments")) / col("views") * 100
        )
    )

    # Quality flags
    df = (
        df
        .withColumn(
            "is_likes_gt_views",
            col("likes") > col("views")
        )
        .withColumn(
            "is_comments_gt_views",
            col("comments") > col("views")
        )
        .withColumn(
            "is_engagement_rate_gt_100",
            col("engagement_rate") > 100
        )
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

    print("\n===== ENGAGEMENT QUALITY REPORT =====")

    df.select(
        spark_sum(
            when(col("is_likes_gt_views"), 1).otherwise(0)
        ).alias("likes_gt_views"),

        spark_sum(
            when(col("is_comments_gt_views"), 1).otherwise(0)
        ).alias("comments_gt_views"),

        spark_sum(
            when(col("is_engagement_rate_gt_100"), 1).otherwise(0)
        ).alias("engagement_gt_100"),

        spark_sum(
            when(col("is_engagement_rate_mismatch"), 1).otherwise(0)
        ).alias("formula_mismatch")
    ).show(truncate=False)

    print("\n===== TOP ANOMALOUS VIDEOS =====")

    df.filter(
        col("is_likes_gt_views")
        | col("is_comments_gt_views")
        | col("is_engagement_rate_mismatch")
    ).select(
        "video_id",
        "views",
        "likes",
        "comments",
        "engagement_rate",
        "calculated_engagement_rate",
        "is_likes_gt_views",
        "is_comments_gt_views"
    ).orderBy(
        col("engagement_rate").desc()
    ).show(20, truncate=False)

finally:
    spark.stop()
