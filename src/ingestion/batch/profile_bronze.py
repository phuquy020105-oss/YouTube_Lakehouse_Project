
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
    min as spark_min,
    max as spark_max
)

bucket = os.environ["MINIO_BUCKET"]

path = f"s3a://{bucket}/bronze/youtube_csv_raw"

spark = (
    SparkSession.builder
    .appName("YouTube-Bronze-Data-Profile")
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

    df = spark.read.format("delta").load(path)

    print("\n===== 1. GENERAL PROFILE =====")

    print("Total records:", df.count())
    print("Distinct video_id:", df.select("video_id").distinct().count())
    print("Distinct channel_id:", df.select("channel_id").distinct().count())
    print("Distinct category_id:", df.select("category_id").distinct().count())

    print("\n===== 2. NULL PROFILE =====")

    null_profile = df.select(
        *[
            spark_sum(
                when(
                    col(c).isNull() | (col(c) == ""),
                    1
                ).otherwise(0)
            ).alias(c)
            for c in [
                "video_id",
                "channel_id",
                "category_id",
                "collected_at",
                "views",
                "likes",
                "comments"
            ]
        ]
    )

    null_profile.show(truncate=False)

    print("\n===== 3. DUPLICATE VIDEO IDS =====")

    duplicates = (
        df.groupBy("video_id")
        .count()
        .filter(col("count") > 1)
    )

    print("Duplicate video_id groups:", duplicates.count())

    duplicates.show(10, truncate=False)

    print("\n===== 4. NUMERIC VALIDATION =====")

    for column in ["views", "likes", "comments"]:
        invalid = df.filter(
            col(column).isNotNull()
            & (col(column).cast("long").isNull())
        ).count()

        negative = df.filter(
            col(column).cast("long") < 0
        ).count()

        print(f"{column}: invalid={invalid}, negative={negative}")

    print("\n===== 5. COLLECTION TIME =====")

    df.select(
        spark_min("collected_at").alias("min_collected_at"),
        spark_max("collected_at").alias("max_collected_at")
    ).show(truncate=False)

    print("\n===== 6. CATEGORY PROFILE =====")

    df.groupBy("category_name").count().show()

    print("\nBRONZE DATA PROFILING: SUCCESS")

finally:
    spark.stop()
