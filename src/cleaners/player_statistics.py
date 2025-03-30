import pandas as pd
from dotenv import load_dotenv
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from cleaner_functions import (
    log_text,
    flush_log_to_s3,
    log_lines, 
    clean_we_ss_pbps,
    clean_kp_kg
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

cleaning_map = {
    "winners-errors": clean_we_ss_pbps,
    "serve-speed": clean_we_ss_pbps,
    "pbp-stats": clean_we_ss_pbps,
    "pbp-points": clean_kp_kg,
    "pbp-games": clean_kp_kg
}

prefix = "raw/player_statistics/"
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if not key.endswith(".parquet"):
        continue # skip non parquet

    parts = key.split("/")
    if len(parts) != 4:
        continue  # based on our file naming formates

    player_folder = parts[2]  # player folder
    file_name = parts[3].replace(".parquet", "") 
    log_text(f"Processing {key}...")

    clean_fn = cleaning_map.get(file_name)

    response = s3_client.get_object(Bucket=s3_bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    table = pq.read_table(buffer)
    df = table.to_pandas()

    cleaned_df = clean_fn(df)

    output_key = key.replace("raw/", "processed/")
    output_buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
    s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

    log_text(f"Cleaned and saved to {output_key}")

flush_log_to_s3("clean/player_statistics_log")
log_lines.clear()