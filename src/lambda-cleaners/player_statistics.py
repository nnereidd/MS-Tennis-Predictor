import pandas as pd
import json
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import io
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from cleaner_functions import (
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
    clean_winners_errors,
    clean_serve_speed,
    clean_pbp_stats,
    clean_pbp_points,
    clean_pbp_games
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
    "winners-errors": clean_winners_errors,
    "serve-speed": clean_serve_speed,
    "pbp-stats": clean_pbp_stats,
    "pbp-points": clean_pbp_points,
    "pbp-games": clean_pbp_games
}

prefix = "raw/player_statistics/"
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)