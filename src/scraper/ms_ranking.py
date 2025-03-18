import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os

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
    df = df.head(201)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
        print(df)

else:
    print(f"Failed to retrieve data: {response.status_code}")


