# YouTube Data Lakehouse

Dự án xây dựng nền tảng YouTube Data Lakehouse theo kiến trúc **Bronze
-- Silver -- Gold**, sử dụng Apache Spark, Delta Lake, Silo
(S3-compatible object storage), Kafka, Debezium và Apache Airflow.

**Phạm vi bàn giao:** hoàn thành đến Gold Layer. Trino và BI là phần
tiếp nối do thành viên khác phụ trách.

## 1. Mục tiêu

-   Nạp dữ liệu YouTube từ bốn file CSV.
-   Thử nghiệm Change Data Capture (CDC) từ PostgreSQL qua Debezium và
    Kafka.
-   Tổ chức dữ liệu theo ba tầng Bronze, Silver và Gold.
-   Lưu bảng Delta Lake trên Silo object storage.
-   Dùng Airflow điều phối các Spark job.
-   Cung cấp Star Schema ở Gold cho bước truy vấn và BI tiếp theo.

## 2. Công nghệ

  Thành phần                    Phiên bản đã kiểm thử
  ----------------------------- --------------------------
  Apache Spark                  3.5.3
  Delta Lake                    Tích hợp với Spark image
  Python / Java                 Python 3.11 / OpenJDK 17
  Apache Kafka                  4.1.2, KRaft
  Debezium Connect              3.7
  PostgreSQL                    15
  Hive Metastore                4.1.0
  Apache Airflow                2.10.5
  Airflow Docker Provider       4.0.0
  Airflow PostgreSQL Provider   6.0.0
  Object storage                Silo, S3-compatible
  Container runtime             Docker Desktop

Các phiên bản trên phản ánh môi trường phát triển đã kiểm thử; khi triển
khai máy khác cần xác nhận lại khả năng tải image và tính tương thích.

## 3. Kiến trúc

``` text
CSV files -> Spark batch --------------------+
                                              v
PostgreSQL -> Debezium -> Kafka -> Spark -> BRONZE
                                              |
                                              v
                                            SILVER
                                              |
                                              v
                                             GOLD
                                      (Star Schema)

Airflow điều phối các Spark jobs bằng DockerOperator.
Delta tables được lưu trên Silo qua giao thức S3A.
```

### Nhánh CSV

``` text
4 CSV files
  -> bronze/youtube_csv_raw
  -> silver_youtube_videos
     silver_channels
     silver_categories
  -> dim_video
     dim_channel
     dim_category
     fact_video_daily
```

### Nhánh CDC

``` text
PostgreSQL table
  -> Debezium PostgreSQL connector
  -> Kafka topic
  -> bronze/youtube_cdc_events
  -> silver_cdc_events
     silver_video_current
```

**Giới hạn CDC hiện tại:** connector và các job CDC đã được thử nghiệm
bằng bảng `public.youtube_cdc_test`. Đây là dữ liệu kiểm thử độc lập,
không phải CDC của toàn bộ 36.352 video CSV.

## 4. Cấu trúc thư mục chính

``` text
YouTube_Lakehouse/
├── airflow/
│   ├── dags/
│   │   └── youtube_lakehouse_pipeline.py
│   ├── logs/
│   └── plugins/
├── config/
│   ├── debezium/
│   │   ├── debezium.properties
│   │   └── postgres-connector.json
│   └── hive/
│       ├── hive-site.xml
│       └── lib/
│           └── postgresql-42.7.5.jar
├── data/
│   ├── raw/
│   └── checkpoints/
├── docker/
│   └── airflow/
│       └── Dockerfile
├── scripts/
│   └── init/
├── src/
│   ├── common/
│   ├── ingestion/
│   │   ├── batch/
│   │   └── streaming/
│   ├── lakehouse/
│   │   ├── silver/
│   │   └── gold/
│   ├── ml/
│   └── streaming/
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

Cấu trúc trên là cấu trúc chính được dùng trong quá trình triển khai; có
thể có thêm file phụ trợ trong repository thực tế.

## 5. Dữ liệu đầu vào

  File                                 Category          Số dòng đã nạp
  ------------------------------------ --------------- ----------------
  `youtube_master_Comedy.csv`          Comedy                     8.963
  `youtube_master_Entertainment.csv`   Entertainment              9.133
  `youtube_master_Gaming.csv`          Gaming                     9.052
  `youtube_master_Music.csv`           Music                      9.204
  **Tổng**                                                   **36.352**

Mỗi file có 21 cột dữ liệu gốc, gồm thông tin video, kênh, thời gian
đăng, chỉ số tương tác, tags, description và batch metadata.

## 6. Cấu hình môi trường

### 6.1 Tạo `.env`

Sao chép `.env.example` thành `.env` và điền thông tin riêng của môi
trường triển khai.

Các nhóm biến cần có:

-   PostgreSQL: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`,
    `POSTGRES_PORT`.
-   Silo: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_API_PORT`,
    `MINIO_CONSOLE_PORT`, `MINIO_BUCKET`.
-   Hive: `HIVE_DB_USER`, `HIVE_DB_PASSWORD`, `HIVE_DB_NAME`,
    `HIVE_METASTORE_PORT`.
-   Kafka: `KAFKA_BROKER_ID`, `KAFKA_CLUSTER_ID`, `KAFKA_PORT`.
-   Kafka Connect: `CONNECT_GROUP_ID`, `CONNECT_CONFIG_TOPIC`,
    `CONNECT_OFFSET_TOPIC`, `CONNECT_STATUS_TOPIC`, `CONNECT_PORT`.
-   Airflow: `AIRFLOW_VERSION`, `AIRFLOW_UID`, `AIRFLOW_EXECUTOR`,
    `AIRFLOW_WEBSERVER_PORT`, `AIRFLOW_ADMIN_USER`,
    `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_FERNET_KEY`.

Tạo Fernet key bằng Airflow image:

``` cmd
docker run --rm --entrypoint python youtube-lakehouse-airflow:2.10.5 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Không commit `.env`, mật khẩu, Fernet key hoặc credential Debezium lên
GitHub.

### 6.2 Điều kiện tiên quyết

-   Windows 10/11, Docker Desktop và Docker Compose.
-   Docker Desktop đã bật backend phù hợp (ví dụ WSL 2).
-   Ổ đĩa chứa project được Docker Desktop cho phép bind mount.
-   Đủ RAM và dung lượng cho PostgreSQL, Kafka, Airflow, Hive, Spark
    image và dữ liệu.
-   Các Docker images cần thiết đã được build/pull hoặc có thể tải từ
    registry.

## 7. Khởi chạy hạ tầng

Mở CMD tại thư mục project:

``` cmd
cd /d D:\YouTube_Lakehouse
```

Kiểm tra Compose:

``` cmd
docker compose config -q
docker compose config --services
```

Khởi động các service nền:

``` cmd
docker compose up -d postgres minio kafka hive-metastore connect
```

Kiểm tra:

``` cmd
docker ps
docker inspect youtube-postgres --format "{{.State.Health.Status}}"
docker inspect youtube-kafka --format "{{.State.Health.Status}}"
docker inspect youtube-hive-metastore --format "{{.State.Health.Status}}"
```

`minio-init` là service khởi tạo bucket; nó có thể kết thúc sau khi chạy
thành công và không cần ở trạng thái Up liên tục.

### 7.1 Hive Metastore database

Trong môi trường phát triển, Hive sử dụng database và user riêng trong
PostgreSQL. Trên PostgreSQL volume mới, cần bảo đảm database/user theo
`.env` đã được tạo trước khi khởi động Hive Metastore nếu các script
init chưa tạo chúng.

Ví dụ SQL (thay bằng thông tin cấu hình thực tế):

``` sql
CREATE USER hive_user WITH PASSWORD 'YOUR_HIVE_PASSWORD';
CREATE DATABASE hive_metastore OWNER hive_user;
```

Không chạy lại lệnh tạo nếu database/user đã tồn tại.

### 7.2 Airflow

Khởi tạo metadata database và tài khoản quản trị:

``` cmd
docker compose up airflow-init
```

Sau khi init thành công, khởi động:

``` cmd
docker compose up -d airflow-webserver airflow-scheduler
```

Mở Airflow:

``` text
http://localhost:8080
```

Đăng nhập bằng `AIRFLOW_ADMIN_USER` và `AIRFLOW_ADMIN_PASSWORD` trong
`.env`.

Kiểm tra DAG:

``` cmd
docker exec youtube-airflow-scheduler airflow dags list
docker exec youtube-airflow-scheduler airflow dags list-import-errors
```

## 8. Airflow orchestration

DAG hiện tại: `youtube_lakehouse_pipeline`.

  ----------------------------------------------------------------------------------
  Task ID                             Spark script
  ----------------------------------- ----------------------------------------------
  `bronze_csv_ingestion`              `src/ingestion/batch/csv_to_bronze.py`

  `silver_csv_processing`             `src/lakehouse/silver/build_silver_csv.py`

  `gold_star_schema`                  `src/lakehouse/gold/build_gold.py`

  `bronze_cdc_ingestion`              `src/ingestion/streaming/kafka_to_bronze.py`

  `silver_cdc_processing`             `src/lakehouse/silver/silver_cdc.py`
  ----------------------------------------------------------------------------------

Dependencies:

``` text
bronze_csv_ingestion
        |
        v
silver_csv_processing
        |
        v
gold_star_schema

bronze_cdc_ingestion
        |
        v
silver_cdc_processing
```

DAG dùng `schedule=None`, nghĩa là chạy thủ công. Trong Airflow UI, bật
DAG nếu đang Pause rồi chọn Trigger DAG.

Xem log bằng Task Instance \> Log hoặc trong `airflow/logs/`.

**Lưu ý khi chạy lại:** các job CSV/Silver/Gold hiện dùng `overwrite`.
Chưa thiết lập lịch chạy định kỳ cho đến khi có incremental
ingestion/upsert và chính sách xử lý batch. Bronze CDC dùng checkpoint;
không tự ý xóa checkpoint khi chạy lại.

## 9. Các tầng dữ liệu

### 9.1 Bronze

``` text
s3a://youtube-lakehouse/bronze/youtube_csv_raw
s3a://youtube-lakehouse/bronze/youtube_cdc_events
```

-   `youtube_csv_raw`: dữ liệu CSV gốc, có metadata nguồn và thời điểm
    ingestion.
-   `youtube_cdc_events`: Kafka raw JSON cùng topic, partition, offset
    và timestamp.

### 9.2 Silver

CSV tables:

``` text
s3a://youtube-lakehouse/silver/silver_youtube_videos
s3a://youtube-lakehouse/silver/silver_channels
s3a://youtube-lakehouse/silver/silver_categories
```

CDC tables:

``` text
s3a://youtube-lakehouse/silver/silver_cdc_events
s3a://youtube-lakehouse/silver/silver_video_current
```

Silver CSV chuẩn hóa kiểu dữ liệu và timestamp, giữ bản ghi thiếu
`collected_at`, đồng thời bổ sung các cờ chất lượng:

-   `is_collected_at_missing`
-   `is_likes_gt_views`
-   `is_comments_gt_views`
-   `is_engagement_rate_gt_100`
-   `is_zero_views_with_interactions`
-   `is_engagement_rate_mismatch`

Các bản ghi bất thường được giữ lại và đánh dấu, không tự động xóa.

### 9.3 Gold -- Star Schema

Các bảng:

``` text
s3a://youtube-lakehouse/gold/dim_video
s3a://youtube-lakehouse/gold/dim_channel
s3a://youtube-lakehouse/gold/dim_category
s3a://youtube-lakehouse/gold/fact_video_daily
```

**Dimensions**

`dim_video`: `video_id`, `title`, `channel_id`, `published_at`.

`dim_channel`: `channel_id`, `channel_title`.

`dim_category`: `category_id`, `category_name`.

**Fact**

`fact_video_daily`:

-   `video_id` -- liên kết `dim_video`.
-   `channel_id` -- liên kết `dim_channel`.
-   `category_id` -- liên kết `dim_category`.
-   `collected_at` -- thời điểm thu thập.
-   `views`, `likes`, `comments`, `engagement_rate` -- measures.

**Grain:** một dòng Fact đại diện cho một video tại một lần thu thập dữ
liệu. Grain logic được kiểm tra bằng cặp `video_id + collected_at`.
Delta Lake không tự cưỡng chế primary key/foreign key như RDBMS.

Dữ liệu hiện tại có một lần thu thập cho mỗi video; không suy diễn thành
lịch sử tăng trưởng theo ngày.

## 10. Kết quả kiểm thử đã ghi nhận

### Bronze CSV

Tổng 36.352 dòng: - Comedy: 8.963. - Entertainment: 9.133. - Gaming:
9.052. - Music: 9.204.

### Silver CSV

  Bảng                        Số dòng
  ------------------------- ---------
  `silver_youtube_videos`      36.352
  `silver_channels`             6.255
  `silver_categories`               4

Kiểm tra chất lượng: - Duplicate `video_id`: 0. - NULL `video_id`,
`channel_id`, `category_id`: 0. - NULL `collected_at`: 407. - Numeric
conversion errors: 0. - Channel/category name conflicts: 0. - Missing
dimension references: 0.

Các bất thường được giữ và đánh dấu: - Likes \> Views: 3 video. -
Comments \> Views: 2 video. - Engagement rate \> 100%: 1 video. - Views
= 0 nhưng có tương tác: 2 video. - Engagement formula mismatch: 0.

### Silver CDC

Bộ dữ liệu CDC thử nghiệm: - Bronze messages: 5. - Tombstone: 1. -
Silver events: 4. - Operations: 1 INSERT, 2 UPDATE, 1 DELETE. -
`silver_video_current`: 0 dòng vì sự kiện cuối cùng là DELETE.

### Gold

  Bảng                   Số dòng
  -------------------- ---------
  `dim_video`             36.352
  `dim_channel`            6.255
  `dim_category`               4
  `fact_video_daily`      36.352

Kiểm tra: - Duplicate dimension keys: 0. - Duplicate Fact grain: 0. -
Missing video/channel/category references: 0. - NULL Fact foreign keys:
0. - NULL `collected_at`: 407.

### Airflow integration

DAG `youtube_lakehouse_pipeline` đã được nhận diện không có import error
và một lần integration test đã hoàn thành cả 5 task ở trạng thái
Success.

## 11. Kiểm tra Gold độc lập

Spark image đã dùng trong môi trường phát triển:

``` text
youtube-lakehouse-spark:3.5.3
```

Chạy:

``` cmd
docker run --rm --network youtube_lakehouse_default --env-file .env -v "%cd%\src:/opt/project/src" youtube-lakehouse-spark:3.5.3 python /opt/project/src/ingestion/batch/validate_gold.py
```

Kết quả cuối mong đợi:

``` text
GOLD VALIDATION: SUCCESS
All four Gold tables passed validation.
```

## 12. Bàn giao cho Trino và BI

Phạm vi repository này kết thúc ở Gold. Thành viên phụ trách tiếp theo
có thể sử dụng:

-   `dim_video`
-   `dim_channel`
-   `dim_category`
-   `fact_video_daily`

Thông tin tích hợp: - S3 endpoint trong Docker network:
`http://minio:9000`. - Bucket: giá trị `MINIO_BUCKET`. - Gold prefix:
`gold/`. - Table format: Delta Lake.

Cần xác nhận phiên bản Trino và Delta Lake connector tương thích với
bảng được tạo bởi Spark/Delta. README này không khẳng định Trino/BI đã
được triển khai hoàn chỉnh.

## 13. GitHub và bảo mật

Trước khi push:

1.  Kiểm tra `.gitignore`.
2.  Không commit `.env`, mật khẩu, Fernet key hoặc credential Debezium.
3.  Không commit Airflow logs và checkpoint runtime.
4.  Quyết định cách phân phối bốn CSV khoảng 107 MB: Git LFS, object
    storage hoặc hướng dẫn tải riêng.
5.  Kiểm tra đường dẫn tuyệt đối trong DAG. Bản phát triển hiện dùng
    `D:\YouTube_Lakehouse`; cần chuyển thành cấu hình portable khi triển
    khai máy khác.

Lệnh Git tham khảo:

``` cmd
git status
git add .
git commit -m "Build YouTube Lakehouse Bronze Silver Gold pipeline"
git push origin main
```

Không chạy `git add .` trước khi xác nhận `.gitignore` đã loại trừ file
bí mật, log, checkpoint và dữ liệu không định đưa lên Git.

## 14. Việc cần hoàn thiện trước production

Các giới hạn đã biết của phiên bản hiện tại:

-   Chuyển CSV/Silver/Gold từ full overwrite sang incremental hoặc
    MERGE.
-   Xác định chiến lược deduplication và xử lý batch mới.
-   Hoàn thiện CDC liên tục, checkpoint và cập nhật current state theo
    transaction/LSN.
-   Không dùng `youtube_cdc_test` làm dữ liệu CDC sản xuất.
-   Chuyển đường dẫn Windows tuyệt đối trong DAG thành cấu hình
    portable.
-   Bổ sung backup/restore, monitoring và hướng dẫn vận hành.
-   Kiểm thử triển khai từ repository sạch trên máy khác.

**Trạng thái:** Bronze, Silver, Gold và Airflow orchestration đã được
kiểm thử trong môi trường phát triển. Dự án sẵn sàng cho bước đóng gói
và bàn giao phần Gold; chưa nên xem là cấu hình production hoàn chỉnh.
