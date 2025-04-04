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
    clean_mcp
)

def main():
    try:
        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

        prefix = "raw/match_charting_project/"
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

            response = s3_client.get_object(Bucket=s3_bucket, Key=key)
            buffer = io.BytesIO(response["Body"].read())
            table = pq.read_table(buffer)
            df = table.to_pandas()

            cleaned_df = clean_mcp(df)

            output_key = key.replace("raw/", "processed/")
            output_buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
            s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

            log_text(f"Cleaned and saved to {output_key}")

    except Exception as e:
            log_text(f"ERROR in match_charting_project.py: {str(e)}")
            raise
    
    finally:
        flush_log_to_s3("clean/match_charting_project_log")
        log_lines.clear()