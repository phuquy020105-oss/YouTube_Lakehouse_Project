
import os
from pyspark.sql import SparkSession

endpoint = "http://minio:9000"
access_key = os.environ["MINIO_ROOT_USER"]
secret_key = os.environ["MINIO_ROOT_PASSWORD"]
bucket = os.environ["MINIO_BUCKET"]

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-S3A-Test")
    .master("local[1]")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel("WARN")

    data = [
        (1, "Music", 150000, 12000),
        (2, "Gaming", 230000, 18000),
        (3, "Comedy", 180000, 15000),
    ]

    df = spark.createDataFrame(
        data,
        ["video_id", "category", "views", "likes"]
    )

    path = f"s3a://{bucket}/bronze/test_s3a"

    print("Writing Parquet to Silo...")
    df.write.mode("overwrite").parquet(path)

    print("Reading Parquet from Silo...")
    result = spark.read.parquet(path)

    result.show(truncate=False)

    assert result.count() == 3

    print("S3A CONNECTION: SUCCESS")
    print("PARQUET WRITE: SUCCESS")
    print("PARQUET READ: SUCCESS")

finally:
    spark.stop()
