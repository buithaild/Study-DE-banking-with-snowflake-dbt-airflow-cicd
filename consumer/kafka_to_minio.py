import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

topics = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]

# Kafka consumer settings
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000")),
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket=bucket)


def write_to_minio(table_name, records):
    if not records:
        return 0

    df = pd.DataFrame(records)
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = f"{table_name}_{date_str}.parquet"
    df.to_parquet(file_path, engine="fastparquet", index=False)

    s3_key = (
        f"{table_name}/date={date_str}/"
        f'{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    )
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f"Uploaded {len(records)} records to s3://{bucket}/{s3_key}")
    return len(records)


batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "50"))
buffer = {topic: [] for topic in topics}
consumed_counts = defaultdict(int)
uploaded_counts = defaultdict(int)

print("Connected to Kafka. Listening for messages...")

try:
    for message in consumer:
        topic = message.topic
        event = message.value
        payload = event.get("payload", {})
        record = payload.get("after")

        if record:
            buffer[topic].append(record)
            consumed_counts[topic] += 1
            print(f"[{topic}] -> {record}")

        if len(buffer[topic]) >= batch_size:
            uploaded_counts[topic] += write_to_minio(topic.split(".")[-1], buffer[topic])
            buffer[topic] = []
finally:
    for topic, records in buffer.items():
        uploaded_counts[topic] += write_to_minio(topic.split(".")[-1], records)
    consumer.close()

print("Kafka consumer finished after idle timeout.")
for topic in topics:
    print(f"{topic}: consumed={consumed_counts[topic]}, uploaded={uploaded_counts[topic]}")
