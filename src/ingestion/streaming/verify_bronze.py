
import os

from pyspark.sql import SparkSession


bucket = os.environ["MINIO_BUCKET"]

path = f"s3a://{bucket}/bronze/youtube_cdc_events"

spark = (
    SparkSession.builder
    .appName("Verify-Bronze-CDC")
    .master("local[1]")
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
    df = spark.read.format("delta").load(path)

    print("\nBRONZE DATA:")
    df.show(truncate=False)

    print("Total CDC events:", df.count())

    print("\nBRONZE SCHEMA:")
    df.printSchema()

finally:
    spark.stop()
