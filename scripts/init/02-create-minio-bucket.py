
import os
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
access_key = os.environ["MINIO_ROOT_USER"]
secret_key = os.environ["MINIO_ROOT_PASSWORD"]
bucket_name = os.getenv("MINIO_BUCKET", "youtube-lakehouse")

client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
)

for attempt in range(30):
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"Bucket already exists: {bucket_name}")
        break
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )

        if status == 404:
            client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
            break

        raise
    except EndpointConnectionError:
        if attempt == 29:
            raise

        print("Waiting for MinIO...")
        time.sleep(2)
