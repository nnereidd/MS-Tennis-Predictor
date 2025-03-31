import pandas as pd
import json
from bs4 import BeautifulSoup
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from functions import (
    scrape_webpage,
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
)

def main():
    try:
        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

        batch_num = int(os.environ.get("BATCH_NUM", 0))  # default to 0 if not set
        batch_size = 25

        start = batch_size * batch_num # scrape in batches
        end = start + batch_size

        player_id_file = "raw/rankings/player_list.json"
        player_id_obj = s3_client.get_object(Bucket=s3_bucket, Key=player_id_file)
        player_list = json.loads(player_id_obj["Body"].read())
        df_player_ids = pd.DataFrame(player_list)

        url_ids = ["winners-errors", "serve-speed", "pbp-games", "pbp-points", "pbp-stats"]

        for index, row in df_player_ids.iloc[start:end].iterrows(): 
            player_id = str(row["player_id"])
            player_name = row["Player"].strip().replace(" ", "").lower()  # clean name for S3 path
            folder_name = f"{player_name}-{player_id}"

            for url_id in url_ids: # loop through the respective pages per player
                try:
                    log_text(f"Scraping {player_id} ({player_name})-{url_id}")
                    df_stats = scrape_webpage(player_id, url_id)

                    buffer = io.BytesIO()
                    df_stats.to_parquet(buffer, engine="pyarrow", index=False) # df to parquet
                    buffer.seek(0)

                    # uploads
                    s3_path = f"raw/player_statistics/{folder_name}/{url_id}.parquet"
                    s3_client.upload_fileobj(buffer, s3_bucket, s3_path)
                    log_text(f"Uploaded {s3_path}")

                    log_scraped_data(df_stats, f"{player_id}-{url_id}", f"raw/player_statistics/{folder_name}")
                    log_text(f"Logged ACTUAL DATA into logs/raw/player_statistics/{folder_name}")

                except Exception as e:
                    log_text(f"Error scraping {player_id}({player_name})-{url_id}: {e}")
                    continue

    except Exception as e:
        log_text(f"ERROR in player_statistics.py: {str(e)}")
        raise

    finally:
        flush_log_to_s3("scrape/player_statistics_log")
        log_lines.clear()