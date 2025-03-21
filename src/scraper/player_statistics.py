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
from scraper_functions import scrape_player_statistics
from scraper_functions import log_scraped_data
from scraper_functions import log_text
from scraper_functions import flush_log_to_s3

load_dotenv()
s3_bucket = os.getenv("S3_BUCKET")
s3_key = os.getenv("AWS_MS_RANKING")
s3_client = boto3.client(
    "s3",
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name= os.getenv("AWS_REGION")
)

try:
    player_id_file = "raw/rankings/ms_rankings.parquet"
    player_id_obj = s3_client.get_object(Bucket=s3_bucket, Key=player_id_file)
    parquet_bytes = io.BytesIO(player_id_obj["Body"].read())
    df_player_ids = pd.read_parquet(parquet_bytes, engine="pyarrow") 

except Exception as e:
    print("error")
    raise

finally:
    print(df_player_ids)



