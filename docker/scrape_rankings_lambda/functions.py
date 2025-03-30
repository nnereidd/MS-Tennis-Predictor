import os
import io
from datetime import datetime
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from bs4 import BeautifulSoup

s3_client = boto3.client("s3")
s3_bucket = os.environ["S3_BUCKET"]

log_lines = []

def make_column_names_unique(headers):
    dup = {}
    new_headers = []
    for col in headers:
        if col in dup:
            dup[col] += 1
            new_headers.append(f"{col}.{dup[col]}")
        else:
            dup[col] = 0
            new_headers.append(col)
    return new_headers

def log_scraped_data(df, file_name, path_name):
    # uploads a timestamped version of the df to the s3 log folder

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    buffer.seek(0)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"logs/{path_name}/{file_name}_{timestamp}.parquet"

    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=buffer.getvalue())
    print(f"Logged {file_name} into: logs/{path_name}")

def log_text(message: str):
    # append message to list (will be for logging data messages)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines.append(f"[{timestamp}] {message}")

def flush_log_to_s3(file_prefix="scrape_log"):
    # uploads log messages on the list to S3 as txt file

    if not log_lines:
        return  # nothing to upload

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines.append(f"[{timestamp}] Log text file uploaded to: s3")
    full_log = "\n".join(log_lines)
    buffer = io.BytesIO(full_log.encode("utf-8"))

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"logs/text_logs/{file_prefix}_{timestamp}.txt"

    buffer.seek(0)
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=buffer.getvalue())
    print(f"Log text file uploaded to: s3")