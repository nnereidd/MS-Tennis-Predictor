import requests
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import io
from scraper_functions import log_scraped_data

load_dotenv()
s3_bucket = os.getenv("S3_BUCKET")
s3_key = os.getenv("AWS_MS_RANKING")
s3_client = boto3.client(
    "s3",
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name= os.getenv("AWS_REGION")
)

url = "https://tennisabstract.com/reports/atp_elo_ratings.html"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0'
}
response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")

    # table has id: reportable
    table = soup.find("table", {"id": "reportable"})
    rankings = []
    rows = table.find_all("tr")

    for row in rows[1:]:  
        columns = row.find_all("td")
        if len(columns) >= 8:  
            elo_rank = columns[0].text.strip()
            player = columns[1].text.strip()
            age = columns[2].text.strip()
            elo = columns[3].text.strip()  # text by indexing td
            hard_elo_rank = columns[5].text.strip()
            hard_elo = columns[6].text.strip()
            clay_elo_rank = columns[7].text.strip()
            clay_elo = columns[8].text.strip()
            grass_elo_rank = columns[9].text.strip()
            grass_elo = columns[10].text.strip()
            peak_elo = columns[12].text.strip()
            peak_month = columns[13].text.strip()
            atp_rank = columns[15].text.strip()
            log_diff = columns[16].text.strip()

            rankings.append([elo_rank, player, age, elo, hard_elo_rank, hard_elo, clay_elo_rank, clay_elo, grass_elo_rank, grass_elo,
                             peak_month, atp_rank, log_diff])

    df = pd.DataFrame(rankings, columns=["Elo Rank", "Player", "Age", "Elo", "Hard Elo Rank", "Hard Elo", 
                                         "Clay Elo Rank", "Clay Elo", "Grass Elo Rank", "Grass Elo", 
                                         "Peak Month", "Atp Rank", "Log Diff"])
    df_ranking = df.head(150).copy() 

    # since the scraped uses /xa0 (non breaking space)
    df_ranking["Player"] = df_ranking["Player"].str.replace("\xa0", " ", regex=True)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
    #     print(df_ranking)

    # access the player list (id)
    player_id_file = "atp_players.csv"
    player_id_obj = s3_client.get_object(Bucket=s3_bucket, Key=player_id_file)
    df_player_ids = pd.read_csv(io.BytesIO(player_id_obj["Body"].read()), dtype={"wikidata_id": str})

    # format player_id and names
    df_player_ids["Player"] = df_player_ids["name_first"] + " " + df_player_ids["name_last"]
    df_player_ids = df_player_ids[["Player", "player_id"]]
    df_player_ids["player_id"] = df_player_ids["player_id"].astype(str)

    df_merged = df_ranking.merge(df_player_ids, how="left", on="Player")
    df_merged = df_merged.dropna(subset=["player_id"])  # drop players w no id
    df_merged["player_id"] = df_merged["player_id"].astype(str)  

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
    #     print(df_merged)

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df_merged)
    pq.write_table(table, buffer) # parquet to bucket

    buffer.seek(0) 
    s3_client.put_object(Bucket=s3_bucket, Key="raw/rankings/ms_rankings.parquet", Body=buffer.getvalue())  
    print("Logged into: raw/rankings/")

    log_scraped_data(df_merged, "ms_rankings.parquet")

else:
    print(f"Failed to retrieve data: {response.status_code}")


