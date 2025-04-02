import pandas as pd
import json
from bs4 import BeautifulSoup
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from functions import (
    scrape_h2h_with_retry,
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
)
import time 
import random

def main():
    try:
        s3_client = boto3.client("s3")
        s3_bucket = os.environ["S3_BUCKET"]

        batch_num = int(os.environ.get("BATCH_NUM", 0))  # default to 0 if not set
        batch_size = 5

        start = batch_size * batch_num # scrape in batches
        end = start + batch_size

        player_id_file = "raw/rankings/player_list.json"
        player_id_obj = s3_client.get_object(Bucket=s3_bucket, Key=player_id_file)
        player_list = json.loads(player_id_obj["Body"].read())
        df_player_ids = pd.DataFrame(player_list)

        for index, row in df_player_ids.iloc[start:end].iterrows(): 
            player_id = str(row["player_id"])
            player_url_name = str(row["Player"])
            player_name = row["Player"].strip().lower()  # clean name for S3 path
            folder_name = f"{player_name}-{player_id}"

            df_opponents = df_player_ids[df_player_ids["player_id"] != player_id] # get player_id of opponents
            
            for _, opponent_row in df_opponents.iloc[start:end].iterrows():
                opponent_url_name = str(opponent_row["Player"])
                opponent_name = opponent_row["Player"].strip().lower()
                opponent_id = str(opponent_row["player_id"])
                try:
                    df_h2h = scrape_h2h_with_retry(player_url_name, opponent_url_name)

                    if df_h2h.empty:
                        log_text(f"No H2H data found for {player_name} vs {opponent_name}, skipping upload.")
                        continue

                    buffer = io.BytesIO()
                    df_h2h.to_parquet(buffer, engine="pyarrow", index=False)
                    buffer.seek(0)

                    # uploads
                    s3_path = f"raw/h2h/{folder_name}/{opponent_name}-{opponent_id}.parquet"
                    s3_client.upload_fileobj(buffer, s3_bucket, s3_path)
                    log_text(f"Uploaded {s3_path}")

                    log_scraped_data(df_h2h, f"{player_name}-{player_id}_vs_{opponent_name}-{opponent_id}", f"raw/h2h/{folder_name}")
                    log_text(f"Logged ACTUAL DATA into logs/raw/h2h/{folder_name}")

                except Exception as e:
                    log_text(f"Error scraping {player_name}-{player_id} vs {opponent_name}-{opponent_id}: {e}")
                    continue

            time.sleep(random.uniform(2, 4)) # for website's server safety

    except Exception as e:
        log_text(f"ERROR in h2h.py: {str(e)}")
        raise

    finally:
        flush_log_to_s3("scrape/h2h_log")
        log_lines.clear()