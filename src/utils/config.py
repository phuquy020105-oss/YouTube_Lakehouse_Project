import os
from dotenv import load_dotenv

load_dotenv()

# Cấu hình kết nối MinIO / S3 Standalone
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Cấu hình Hive Metastore
HIVE_METASTORE_URIS = os.getenv("HIVE_METASTORE_URIS", "thrift://localhost:9083")

# Cấu hình Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "youtube_videos_topic")
