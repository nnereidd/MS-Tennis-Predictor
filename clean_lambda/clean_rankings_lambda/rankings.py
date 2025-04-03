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
    clean_column_name
)

def main():
    try:

        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

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

            if 'player' in df.columns:
                df['player'] = df['player'].astype(str).str.lower().str.strip()

            df = df.replace(r'^\s*$|^–$|^-$', pd.NA, regex=True)

            for col in df.columns:
                if col != "player":  # skip string column
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass  # skip non-numeric conversions

            cleaned_df = df

            output_key = key.replace("raw/", "processed/")
            output_buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pandas(cleaned_df), output_buffer, compression="snappy")
            s3_client.put_object(Bucket=s3_bucket, Key=output_key, Body=output_buffer.getvalue())

            log_text(f"Cleaned and saved to {output_key}")

    except Exception as e:
            log_text(f"ERROR in rankings.py: {str(e)}")
            raise

    finally:
        flush_log_to_s3("clean/rankings_log")
        log_lines.clear()