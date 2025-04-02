import pandas as pd
from dotenv import load_dotenv
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from functions import (
    log_text,
    flush_log_to_s3,
    log_lines, 
    clean_column_name
)

load_dotenv()
s3_bucket = os.getenv("S3_BUCKET")
s3_key = os.getenv("AWS_MS_RANKING")
s3_client = boto3.client(
    "s3",
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name= os.getenv("AWS_REGION")
)

prefix = "raw/rankings/"
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if not key.endswith(".parquet"):
        continue # skip non parquet
    log_text(f"Processing {key}...")

    response = s3_client.get_object(Bucket=s3_bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    table = pq.read_table(buffer)
    df = table.to_pandas()

    df.columns = [clean_column_name(col) for col in df.columns]

    for col in df.columns: 
        if col == 'player':
            df[col] = df[col].astype(str).str.lower().str.strip()
        cleaned_df = df.replace(r'^\s*$|^–$|^-$', pd.NA, regex=True)

    output_key = key.replace("raw/", "processed/")
    output_buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
    s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

    log_text(f"Cleaned and saved to {output_key}")

flush_log_to_s3("clean/rankings_log")
log_lines.clear()