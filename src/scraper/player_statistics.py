import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
import os

driver_path = os.getenv("EDGE_DRIVER_PATH")
options = webdriver.EdgeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--log-level=3") 

# edge webDriver
service = Service(driver_path)
driver = webdriver.Edge(service=service, options=options)

url = "https://www.tennisabstract.com/cgi-bin/player-more.cgi?p=206173/Jannik-Sinner&table=winners-errors"
driver.get(url)
html = driver.page_source
driver.quit()

# parse through html
soup = BeautifulSoup(html, "html.parser")
table = soup.find("table", {"id": "winners-errors"})
rows = table.find_all("tr")

data = []
headers = [header.text.strip() for header in rows[0].find_all("th")]

for row in rows[1:]:
    columns = [col.text.strip() for col in row.find_all("td")]  # loop through table rows
    if columns:
        data.append(columns)

# put data into dataframe
df = pd.DataFrame(data, columns=headers)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df)  




