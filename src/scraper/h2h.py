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
from scraper_functions import (
    log_scraped_data,
    log_text,
    flush_log_to_s3,
    log_lines, 
    get_edge_driver
)

driver = get_edge_driver()
url = f"https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p=JannikSinner&f=ACareerqqw1&q=BenShelton&q=BenShelton"
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