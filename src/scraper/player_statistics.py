import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from scraper_functions import (
    scrape_player_statistics,
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

url_ids = ["winners-errors", "serve-speed"]
test_player_ids = ["206173", "104925"]

for player_id in test_player_ids:
    for url_id in url_ids:
        try:
            log_text(f"Scraping {player_id} - {url_id}")
            df_stats = scrape_player_statistics(player_id, url_id)

            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df_stats)
            pq.write_table(table, buffer) # parquet to bucket
            buffer.seek(0) 

            s3_path = f"raw/player_statistics/{player_id}/{url_id}.parquet"
            s3_client.upload_fileobj(buffer, s3_bucket, s3_path)
            log_text(f"Uploaded {s3_path}")

            log_scraped_data(df_stats, f"{player_id} - {url_id}", f"raw/player_statistics/{player_id}")
            log_text(f"Logged ACTUAL DATA {player_id} - {url_id}.parquet into: logs/raw/player_statistics/{player_id}") 

        except Exception as e:
            log_text(f"Error scraping {player_id} - {url_id}: {e}")
            continue 

flush_log_to_s3("player_statistics_log")
log_lines.clear()



