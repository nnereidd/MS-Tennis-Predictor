import time
import random
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os
import json
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import io
from scraper_functions import (
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
    get_edge_driver
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

for index, row in df_player_ids.iloc[:2].iterrows(): 
    player_id = str(row["player_id"])
    player_url_name = str(row["Player"])
    player_name = row["Player"].strip().lower()  # clean name for S3 path
    folder_name = f"{player_name}-{player_id}"

    df_opponents = df_player_ids[df_player_ids["player_id"] != player_id] # get player_id of opponents
    
    for _, opponent_row in df_opponents.iloc[:2].iterrows():
        opponent_url_name = str(opponent_row["Player"])
        opponent_name = opponent_row["Player"].strip().lower()
        opponent_id = str(opponent_row["player_id"])
        try:
            driver = get_edge_driver()
            url = f"https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={player_url_name}&f=ACareerqqw1&q={opponent_url_name}&q={opponent_url_name}"
            driver.get(url)
            html = driver.page_source
            driver.quit()

            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", {"id": "matches"})

            rows = table.find_all("tr")
            headers = [header.text.strip() for header in rows[0].find_all("th")]
            data = []

            for row in rows[1:]:
                columns = [col.text.strip() for col in row.find_all("td")]
                if columns:
                    data.append(columns)

            df_h2h = pd.DataFrame(data, columns=headers)

            with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
                print(df_h2h)

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

        time.sleep(random.uniform(1, 3))

flush_log_to_s3("scrape/h2h")
log_lines.clear()