import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, length, hour, dayofweek, month, split, size, when, 
    expr, count, lit, coalesce
)

# Thêm thư mục gốc để gọi file config
sys.path.append(os.getcwd())
from src.utils import config

print(" Khởi tạo Spark Session cho Tầng Gold (Delta Lake + Hive Metastore)...")
spark = SparkSession.builder \
    .appName("YouTube-Lakehouse-Gold") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc dữ liệu sạch từ Tầng Silver (Delta)
silver_path = "s3a://youtube-data/lakehouse/processed/youtube_silver/"
print(f" Đang đọc dữ liệu sạch từ Tầng Silver (Delta): {silver_path}")
df_silver = spark.read.format("delta").load(silver_path)

# Lọc bỏ các bản ghi không hợp lệ hoặc thiếu chỉ số tương tác
df_valid = df_silver.filter(col("engagement_rate_calculated").isNotNull())

print(" 1. Xây dựng bảng: gold_video_analysis (Feature Engineering)...")
# Tạo các đặc trưng về thời gian, text và phân loại thời lượng
df_video_analysis = df_valid \
    .withColumn("publish_hour", hour(col("published_at"))) \
    .withColumn("publish_day_of_week", dayofweek(col("published_at"))) \
    .withColumn("publish_month", month(col("published_at"))) \
    .withColumn("title_length", length(col("title"))) \
    .withColumn("description_length", coalesce(length(col("description")), lit(0))) \
    .withColumn("tags_count", when(col("tags").isNull() | (col("tags") == ""), 0).otherwise(size(split(col("tags"), r"[|,]")))) \
    .withColumn("duration_bucket", 
        when(col("duration_sec") < 60, "Shorts (<1m)")
        .when((col("duration_sec") >= 60) & (col("duration_sec") <= 300), "Short (1-5m)")
        .when((col("duration_sec") > 300) & (col("duration_sec") <= 1200), "Medium (5-20m)")
        .otherwise("Long (>20m)")
    ) \
    .select(
        "video_id", "channel_id", "channel_title", "title", "category_standard",
        "published_at", "publish_hour", "publish_day_of_week", "publish_month",
        "duration_sec", "duration_bucket",
        "title_length", "description_length", "tags_count",
        "views", "likes", "comments", "engagement_rate_calculated",
        "content_status"
    )

print(" 2. Xây dựng bảng: gold_category_summary (Thống kê Trung vị & Phân vị)...")
# Dùng hàm expr("percentile_approx(...)") để tính trung vị (0.5) và phân vị (0.25, 0.75)
df_category_summary = df_video_analysis.groupBy("category_standard").agg(
    count("video_id").alias("video_count"),
    expr("percentile_approx(views, 0.5)").alias("median_views"),
    expr("percentile_approx(likes, 0.5)").alias("median_likes"),
    expr("percentile_approx(comments, 0.5)").alias("median_comments"),
    expr("percentile_approx(duration_sec, 0.5)").alias("median_duration_sec"),
    expr("percentile_approx(engagement_rate_calculated, 0.25)").alias("p25_engagement_rate"),
    expr("percentile_approx(engagement_rate_calculated, 0.5)").alias("median_engagement_rate"),
    expr("percentile_approx(engagement_rate_calculated, 0.75)").alias("p75_engagement_rate")
)

print(" 3. Xây dựng bảng: gold_engagement_ml (Chuẩn bị dữ liệu Model)...")
# Tính ngưỡng chia nhóm Low / Medium / High dựa trên phân vị 33% và 66% của toàn bộ dữ liệu
quantiles = df_video_analysis.approxQuantile("engagement_rate_calculated", [0.3333, 0.6667], 0.01)
low_threshold = quantiles[0]
high_threshold = quantiles[1]

print(f"   -> ML Thresholds: Low <= {low_threshold:.2f}% | Medium | High >= {high_threshold:.2f}%")

df_engagement_ml = df_video_analysis.withColumn(
    "engagement_level",
    when(col("engagement_rate_calculated") <= low_threshold, "Low")
    .when((col("engagement_rate_calculated") > low_threshold) & (col("engagement_rate_calculated") < high_threshold), "Medium")
    .otherwise("High")
).select(
    "video_id", "category_standard", "duration_sec", "publish_hour", "publish_day_of_week", 
    "publish_month", "title_length", "description_length", "tags_count", "engagement_level"
)

# --- LƯU XUỐNG MINIO & ĐĂNG KÝ VÀO HIVE METASTORE (TẦNG GOLD) ---
gold_base_path = "s3a://youtube-data/lakehouse/processed/youtube_gold/"

print(f" Đang lưu gold_video_analysis xuống MinIO và đăng ký Hive Metastore...")
df_video_analysis.write.format("delta").mode("overwrite").option("path", gold_base_path + "video_analysis/").saveAsTable("gold_video_analysis")

print(f" Đang lưu gold_category_summary xuống MinIO và đăng ký Hive Metastore...")
df_category_summary.write.format("delta").mode("overwrite").option("path", gold_base_path + "category_summary/").saveAsTable("gold_category_summary")

print(f" Đang lưu gold_engagement_ml xuống MinIO và đăng ký Hive Metastore...")
df_engagement_ml.write.format("delta").mode("overwrite").option("path", gold_base_path + "engagement_ml/").saveAsTable("gold_engagement_ml")

print(" Hoàn tất xây dựng Tầng Gold Data Mart (Delta format + Hive Catalog) thành công tuyệt đối!")