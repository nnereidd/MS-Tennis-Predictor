import pandas as pd
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from functions import (
    log_text,
    flush_log_to_s3,
    log_lines, 
    clean_we_ss_pbps,
    clean_kp_kg
)

def main():
    try:
        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

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

            file_root, ext = os.path.splitext(parts[3])  # file_name + ".parquet"
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_key = f"processed/player_statistics/{player_folder}/{file_root}_{timestamp}.parquet"

            output_buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
            s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

            log_text(f"Cleaned and saved to {output_key}")

    except Exception as e:
            log_text(f"ERROR in player_statistics.py: {str(e)}")
            raise
    
    finally:
        flush_log_to_s3("clean/player_statistics_log")
        log_lines.clear()