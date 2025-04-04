import pandas as pd
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from functions import (
    log_text,
    flush_log_to_s3,
    log_lines, 
    clean_h2h
)

def main():
    try:
        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

        prefix = "raw/h2h/"
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

            cleaned_df = clean_h2h(df)

            output_key = key.replace("raw/", "processed/")
            output_buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
            s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

            log_text(f"Cleaned and saved to {output_key}")

    except Exception as e:
            log_text(f"ERROR in h2h.py: {str(e)}")
            raise
    
    finally:
        flush_log_to_s3("clean/h2h")
        log_lines.clear()