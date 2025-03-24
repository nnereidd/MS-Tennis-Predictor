import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import json
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from scraper_functions import (
    scrape_webpage,
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
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

player_id_file = "raw/rankings/player_list.json"
player_id_obj = s3_client.get_object(Bucket=s3_bucket, Key=player_id_file)
player_list = json.loads(player_id_obj["Body"].read())
df_player_ids = pd.DataFrame(player_list)

url_ids = ["mcp-serve", "mcp-return", "mcp-rally", "mcp-tactics"]

for index, row in df_player_ids.iloc[:2].iterrows(): 
    player_id = str(row["player_id"])
    player_name = row["Player"].strip().replace(" ", "").lower()  # clean name for S3 path
    folder_name = f"{player_name}-{player_id}"

    for url_id in url_ids: # loop through the respective pages per player
        try:
            log_text(f"Scraping {player_id} ({player_name})-{url_id}")
            df_stats = scrape_webpage(player_id, url_id)

            buffer = io.BytesIO()
            df_stats.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            # uploads
            s3_path = f"raw/match_charting_project/{folder_name}/{url_id}.parquet"
            s3_client.upload_fileobj(buffer, s3_bucket, s3_path)
            log_text(f"Uploaded {s3_path}")

            log_scraped_data(df_stats, f"{player_id}-{url_id}", f"raw/match_charting_project/{folder_name}")
            log_text(f"Logged ACTUAL DATA into logs/raw/match_charting_project/{folder_name}")

        except Exception as e:
            log_text(f"Error scraping {player_id} ({player_name})-{url_id}: {e}")
            continue

flush_log_to_s3("match_charting_project_log")
log_lines.clear()