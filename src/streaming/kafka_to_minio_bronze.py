from pyspark.sql import SparkSession

def create_spark_session():
    # Khởi tạo Spark Session tích hợp Kafka, AWS S3 (MinIO), Delta Lake và Hive Metastore
    spark = SparkSession.builder \
        .appName("YoutubeLakehouse-BronzeLayer-Delta") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://youtube-data/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def consume_kafka_to_minio():
    spark = create_spark_session()
    print(" Đang kết nối tới Kafka topic: dbserver1.public.youtube_videos...")

    # 1. Đọc luồng dữ liệu thời gian thực từ Kafka topic do Debezium tạo ra
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dbserver1.public.youtube_videos") \
        .option("startingOffsets", "earliest") \
        .option("kafka.client.dns.lookup", "use_all_dns_ips") \
        .load()

    # 2. Chuyển đổi giá trị binary từ Kafka sang chuỗi JSON thô
    parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_payload")

    print(" Đang thiết lập luồng hạ cánh dữ liệu xuống MinIO (Tầng Bronze - Delta Lake)...")

    # 3. Ghi streaming xuống MinIO ở tầng Bronze dưới dạng Delta Table
    query = parsed_df.writeStream \
        .format("delta") \
        .option("path", "s3a://youtube-data/bronze/youtube_videos/") \
        .option("checkpointLocation", "s3a://youtube-data/bronze/checkpoint/youtube_videos/") \
        .outputMode("append") \
        .start()

    print("--- Bronze Layer Delta Streaming Pipeline is running ---")
    print(" Hệ thống đang lắng nghe và ghi dữ liệu Delta. Nhấn Ctrl+C trên Terminal để dừng.")
    query.awaitTermination()

if __name__ == "__main__":
    consume_kafka_to_minio()