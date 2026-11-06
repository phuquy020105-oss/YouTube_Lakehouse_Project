

\# Spark Configuration



\## Runtime versions



\- Apache Spark: 3.5.3

\- Python: 3.11

\- Java: 17

\- Hadoop: 3.3.4

\- Delta Lake: 3.3.0

\- Scala: 2.12



\## Files



\### spark-defaults.conf



Contains common configuration references:



\- Delta Lake extensions.

\- Delta Catalog.

\- S3A connector.

\- Silo endpoint.

\- S3 path-style access.

\- UTC timezone.

\- Spark SQL settings.



\### log4j2.properties



Controls Spark logging:



\- Console output.

\- WARN root logging level.

\- Spark, Hadoop, Kafka and Delta log levels.



\## Credentials



Never store credentials in Spark configuration files.



The following environment variables are supplied at runtime:



\- MINIO\_ROOT\_USER

\- MINIO\_ROOT\_PASSWORD

\- MINIO\_BUCKET



\## Runtime configuration



Current Python Spark jobs configure S3A and Delta Lake

through SparkSession.builder.



The configuration files in this directory are

standardized reference configurations.



They are not automatically loaded by every current

Spark job.



Do not remove existing SparkSession configurations

until centralized configuration loading has been

implemented and tested.



\## Docker Image



Spark image:



youtube-lakehouse-spark:3.5.3



Dockerfile:



docker/spark/Dockerfile



