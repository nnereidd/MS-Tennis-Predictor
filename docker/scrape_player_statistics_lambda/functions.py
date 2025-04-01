import os
import io
from datetime import datetime
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from tempfile import mkdtemp

s3_client = boto3.client("s3")
s3_bucket = os.environ["S3_BUCKET"]

log_lines = []

def get_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # important for modern headless Chrome
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")
    
    # Update this path based on where your shell script installs Chrome
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log"
    )

    return webdriver.Chrome(service=service, options=chrome_options)

def make_column_names_unique(headers):
    dup = {}
    new_headers = []
    for col in headers:
        if col in dup:
            dup[col] += 1
            new_headers.append(f"{col}.{dup[col]}")
        else:
            dup[col] = 0
            new_headers.append(col)
    return new_headers

def scrape_webpage(player_id, url_id):
    # function to scrape and iterate through the required pages

    driver = get_chrome_driver()
    url = "https://www.tennisabstract.com/cgi-bin/player-more.cgi?p=" + player_id + "/Jannik-Sinner&table=" + url_id
    driver.get(url)                                                                 # this part is only image
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": url_id})

    rows = table.find_all("tr")
    headers = make_column_names_unique([header.text.strip() for header in rows[0].find_all("th")])
    data = []

    for row in rows[1:]:
        columns = [col.text.strip() for col in row.find_all("td")]
        if columns:
            data.append(columns)

    return pd.DataFrame(data, columns=headers)

def log_scraped_data(df, file_name, path_name):
    # uploads a timestamped version of the df to the s3 log folder

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    buffer.seek(0)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"logs/{path_name}/{file_name}_{timestamp}.parquet"

    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=buffer.getvalue())
    print(f"Logged {file_name} into: logs/{path_name}")

def log_text(message: str):
    # append message to list (will be for logging data messages)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines.append(f"[{timestamp}] {message}")

def flush_log_to_s3(file_prefix="scrape_log"):
    # uploads log messages on the list to S3 as txt file

    if not log_lines:
        return  # nothing to upload

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines.append(f"[{timestamp}] Log text file uploaded to: s3")
    full_log = "\n".join(log_lines)
    buffer = io.BytesIO(full_log.encode("utf-8"))

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"logs/text_logs/{file_prefix}_{timestamp}.txt"

    buffer.seek(0)
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=buffer.getvalue())
    print(f"Log text file uploaded to: s3")