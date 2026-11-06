
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    get_json_object,
    coalesce,
    to_timestamp,
    from_unixtime,
    row_number,
    count
)
from pyspark.sql.window import Window


# =====================================================
# 1. CONFIGURATION
# =====================================================

BUCKET = os.environ["MINIO_BUCKET"]

BRONZE_PATH = (
    f"s3a://{BUCKET}/bronze/youtube_cdc_events"
)

SILVER_EVENTS_PATH = (
    f"s3a://{BUCKET}/silver/silver_cdc_events"
)

SILVER_CURRENT_PATH = (
    f"s3a://{BUCKET}/silver/silver_video_current"
)


# =====================================================
# 2. CREATE SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("YouTubeLakehouse-Silver-CDC")
    .master("local[2]")

    # S3A configuration
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
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
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
# 3. READ BRONZE CDC
# =====================================================

try:
    spark.sparkContext.setLogLevel("WARN")

    print("\n===== READING BRONZE CDC =====")

    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    print("Bronze CDC messages:", bronze_df.count())

    # =================================================
    # 4. REMOVE TOMBSTONE MESSAGES
    # =================================================

    print("\n===== FILTERING TOMBSTONES =====")

    tombstone_count = (
        bronze_df
        .filter(col("raw_value").isNull())
        .count()
    )

    event_source = (
        bronze_df
        .filter(col("raw_value").isNotNull())
    )

    print("Tombstone messages:", tombstone_count)
    print("CDC event messages:", event_source.count())

    # =================================================
    # 5. PARSE DEBEZIUM JSON
    # =================================================

    print("\n===== PARSING DEBEZIUM EVENTS =====")

    parsed_df = (
        event_source

        # Video ID
        .withColumn(
            "video_id",
            coalesce(
                get_json_object(
                    col("raw_value"),
                    "$.payload.after.video_id"
                ),
                get_json_object(
                    col("raw_value"),
                    "$.payload.before.video_id"
                ),
                get_json_object(
                    col("kafka_key"),
                    "$.payload.video_id"
                )
            ).cast("long")
        )

        # Operation
        .withColumn(
            "operation",
            get_json_object(
                col("raw_value"),
                "$.payload.op"
            )
        )

        # Before and after
        .withColumn(
            "before_data",
            get_json_object(
                col("raw_value"),
                "$.payload.before"
            )
        )
        .withColumn(
            "after_data",
            get_json_object(
                col("raw_value"),
                "$.payload.after"
            )
        )

        # Source metadata
        .withColumn(
            "source_database",
            get_json_object(
                col("raw_value"),
                "$.payload.source.db"
            )
        )
        .withColumn(
            "source_schema",
            get_json_object(
                col("raw_value"),
                "$.payload.source.schema"
            )
        )
        .withColumn(
            "source_table",
            get_json_object(
                col("raw_value"),
                "$.payload.source.table"
            )
        )
        .withColumn(
            "connector_version",
            get_json_object(
                col("raw_value"),
                "$.payload.source.version"
            )
        )
        .withColumn(
            "source_lsn",
            get_json_object(
                col("raw_value"),
                "$.payload.source.lsn"
            ).cast("long")
        )

        # Event timestamp
        .withColumn(
            "event_timestamp",
            to_timestamp(
                from_unixtime(
                    get_json_object(
                        col("raw_value"),
                        "$.payload.ts_ms"
                    ).cast("double") / 1000
                )
            )
        )

        # Preserve ingestion metadata
        .withColumn(
            "kafka_offset",
            col("offset")
        )
    )

    # =================================================
    # 6. VALIDATE CDC EVENTS
    # =================================================

    print("\n===== VALIDATING CDC EVENTS =====")

    invalid_video_id = (
        parsed_df
        .filter(col("video_id").isNull())
        .count()
    )

    invalid_operation = (
        parsed_df
        .filter(
            ~col("operation").isin("c", "u", "d")
            | col("operation").isNull()
        )
        .count()
    )

    duplicate_events = (
        parsed_df
        .groupBy("topic", "partition", "offset")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("Invalid video_id:", invalid_video_id)
    print("Invalid operation:", invalid_operation)
    print("Duplicate Kafka messages:", duplicate_events)

    if invalid_video_id > 0:
        raise ValueError("Invalid CDC video_id found")

    if invalid_operation > 0:
        raise ValueError("Invalid CDC operation found")

    if duplicate_events > 0:
        raise ValueError("Duplicate CDC events found")

    # =================================================
    # 7. BUILD SILVER CDC EVENTS
    # =================================================

    print("\n===== BUILDING SILVER CDC EVENTS =====")

    silver_events = parsed_df.select(
        "video_id",
        "operation",
        "before_data",
        "after_data",
        "source_database",
        "source_schema",
        "source_table",
        "connector_version",
        "source_lsn",
        "event_timestamp",
        "topic",
        "partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at"
    )

    (
        silver_events.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_EVENTS_PATH)
    )

    print("Silver CDC Events: SUCCESS")

    # =================================================
    # 8. BUILD SILVER VIDEO CURRENT
    # =================================================

    print("\n===== BUILDING SILVER VIDEO CURRENT =====")

    # Latest event for each video
    latest_window = (
        Window
        .partitionBy("video_id")
        .orderBy(
            col("source_lsn").desc_nulls_last(),
            col("partition").desc(),
            col("kafka_offset").desc()
        )
    )

    latest_events = (
        parsed_df
        .withColumn(
            "event_rank",
            row_number().over(latest_window)
        )
        .filter(col("event_rank") == 1)
    )

    # DELETE events are excluded from current state
    current_source = (
        latest_events
        .filter(col("operation") != "d")
    )

    silver_current = (
        current_source

        # Current video attributes
        .withColumn(
            "title",
            get_json_object(
                col("after_data"),
                "$.title"
            )
        )
        .withColumn(
            "views",
            get_json_object(
                col("after_data"),
                "$.views"
            ).cast("long")
        )
        .withColumn(
            "likes",
            get_json_object(
                col("after_data"),
                "$.likes"
            ).cast("long")
        )
        .withColumn(
            "comments",
            get_json_object(
                col("after_data"),
                "$.comments"
            ).cast("long")
        )
        .withColumn(
            "category",
            get_json_object(
                col("after_data"),
                "$.category"
            )
        )
        .withColumn(
            "updated_at_micros",
            get_json_object(
                col("after_data"),
                "$.updated_at"
            ).cast("long")
        )

        .select(
            "video_id",
            "title",
            "views",
            "likes",
            "comments",
            "category",
            "updated_at_micros",
            col("operation").alias("last_operation"),
            "source_lsn",
            "topic",
            "partition",
            "kafka_offset",
            "event_timestamp"
        )
    )

    (
        silver_current.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_CURRENT_PATH)
    )

    print("Silver Video Current: SUCCESS")

    # =================================================
    # 9. FINAL VALIDATION
    # =================================================

    print("\n===== FINAL CDC VALIDATION =====")

    events_result = (
        spark.read
        .format("delta")
        .load(SILVER_EVENTS_PATH)
    )

    current_result = (
        spark.read
        .format("delta")
        .load(SILVER_CURRENT_PATH)
    )

    total_events = events_result.count()
    current_count = current_result.count()

    print("Silver CDC Events:", total_events)
    print("Silver Video Current:", current_count)

    print("\nOperation distribution:")

    (
        events_result
        .groupBy("operation")
        .count()
        .orderBy("operation")
        .show()
    )

    print("\nCDC event details:")

    events_result.select(
        "video_id",
        "operation",
        "source_database",
        "source_table",
        "source_lsn",
        "kafka_offset"
    ).orderBy("kafka_offset").show(
        truncate=False
    )

    print("\nCurrent video records:")

    current_result.show(truncate=False)

    print("\n" + "=" * 55)
    print("SILVER CDC PIPELINE: SUCCESS")
    print("=" * 55)

finally:
    spark.stop()
