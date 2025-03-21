import os
import io
from datetime import datetime
from dotenv import load_dotenv
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service

load_dotenv()
s3_client = boto3.client("s3")
s3_bucket = os.getenv("S3_BUCKET")

def get_edge_driver():
    # creates the headless edge driver for accessing page

    driver_path = os.getenv("EDGE_DRIVER_PATH")
    options = webdriver.EdgeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    
    service = Service(driver_path)
    return webdriver.Edge(service=service, options=options)

def scrape_nine_pages(player_id, url_id):
    # function to scrape and iterate through the 9 pages

    driver = get_edge_driver()
    url = f"https://www.tennisabstract.com/cgi-bin/player-more.cgi?p={player_id}&table={url_id}"
    driver.get(url)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": url_id})

    rows = table.find_all("tr")
    headers = [header.text.strip() for header in rows[0].find_all("th")]
    data = []

    for row in rows[1:]:
        columns = [col.text.strip() for col in row.find_all("td")]
        if columns:
            data.append(columns)

    return pd.DataFrame(data, columns=headers)

def log_scraped_data(df, file_name):
    # uploads a timestamped version of the df to the s3 log folder

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    buffer.seek(0)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"logs/{file_name}_{timestamp}.parquet"

    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=buffer.getvalue())
    print(f"Logged/{file_name} into: log/")



