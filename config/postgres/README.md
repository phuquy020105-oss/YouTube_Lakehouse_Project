

\# PostgreSQL Configuration



PostgreSQL 15 được sử dụng cho:



\- Airflow Metadata Database.

\- YouTube application database.

\- MLflow tracking database.

\- Debezium CDC source.



\## Logical Replication



Các thông số CDC:



\- wal\_level = logical

\- max\_replication\_slots = 10

\- max\_wal\_senders = 10



\## Current Runtime Configuration



Trong phiên bản hiện tại, PostgreSQL nhận các tham số

logical replication trực tiếp từ docker-compose.yml:



```yaml

command:

&#x20; - postgres

&#x20; - -c

&#x20; - wal\_level=logical

&#x20; - -c

&#x20; - max\_replication\_slots=10

&#x20; - -c

&#x20; - max\_wal\_senders=10

```



File postgresql.conf trong thư mục này là cấu hình

chuẩn hóa và tham chiếu.



Nó chưa được mount vào PostgreSQL container.



Không thay đổi cấu hình runtime nếu chưa kiểm thử

ảnh hưởng đến Debezium và logical replication.



