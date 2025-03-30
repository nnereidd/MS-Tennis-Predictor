import os
import io
from datetime import datetime
from dotenv import load_dotenv
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import re

load_dotenv()
s3_client = boto3.client("s3")
s3_bucket = os.getenv("S3_BUCKET")

log_lines = []

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

def clean_column_name(name):
    return(
        name.replace('\xa0', ' ')
            .replace('/', 'per')
            .replace('%', '')
            .replace(' ', '_')
            .replace(':', '')
            .lower()
            .strip()
    )

def remove_bracketed_text(s):
    if isinstance(s, str):
        return re.sub(r'\s*\([^)]*\)', '', s).strip()
    return s

def clean_match_results(value):
    if not isinstance(value, str):
        return value
    value = value.strip().lower()

    match_win = re.match(r'^w\s+vs\s+(.*)', value)
    match_loss = re.match(r'^l\s+vs\s+(.*)', value)

    if match_win:
        opponent = match_win.group(1).strip().replace(' ', '_')
        opponent_cleaned = re.sub(r'\s+', '', opponent)
        return f"win_{opponent_cleaned}"
    elif match_loss:
        opponent = match_loss.group(1).strip().replace(' ', '_')
        opponent_cleaned = re.sub(r'\s+', '', opponent)
        return f"loss_{opponent_cleaned}"

    return value.replace(' ', '_')

def convert_ratios_percentages(value):
    try:
        if isinstance(value, str):
            value = value.strip()
            # handle percentages
            if value.endswith("%"):
                num = float(value.rstrip("%").strip())
                return num / 100
            
            if "/" in value: # handle ratios
                num, denom = value.split("/")
                num = float(num.strip())
                denom = float(denom.strip())
                return (num / denom)* 100 if denom != 0 else pd.NA

        return pd.to_numeric(value, errors="coerce")

    except:
        return pd.NA

def clean_winner(text):
    winner_raw = re.split(r'\s*d\.\s*', text)[0]
    winner_raw = re.sub(r'\([^)]*\)', '', winner_raw)  # remove ( )
    winner_raw = re.sub(r'\[[^]]*\]', '', winner_raw)  # remove [ ]

    cleaned = re.sub(r'\s+', '_', winner_raw.strip())
    cleaned = cleaned.lower()
    return cleaned

def clean_we_ss_pbps(df):
# cleans winners-errors, serve speed and pbp stats data

    df.columns = [clean_column_name(col) for col in df.columns]

    for col in df.columns:
        if col == "match":
            df[col] = df[col].astype(str).str.replace(r"\s+", "_", regex=True).str.lower().str.strip()

        elif col == "result":
            df[col] = df[col].astype(str).apply(clean_match_results)

        else:
            if df[col].astype(str).str.contains('%').any():
                df[col] = pd.to_numeric(df[col].replace("%", "", regex=True), errors="coerce") / 100
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace(r'^\s*$|^–$|^-$', pd.NA, regex=True)

    return df

def clean_kp_kg(df): # clean key points and key games (due to different values in some columns)

    df.columns = [clean_column_name(col) for col in df.columns]

    for col in df.columns: 
        if df[col].dtype == "object":
            df[col] = df[col].apply(remove_bracketed_text)

        if col == "match":
            df[col] = df[col].astype(str).str.replace(r"\s+", "_", regex=True).str.lower().str.strip()

        elif col == "result":
            df[col] = df[col].astype(str).apply(clean_match_results)
        
        else:
            if df[col].astype(str).str.contains('%').any():
                df[col] = pd.to_numeric(df[col].replace("%", "", regex=True), errors="coerce")/100
            df[col] = df[col].apply(convert_ratios_percentages)

    df = df.replace(r'^\s*$|^–$|^-$|^0/0$', pd.NA, regex=True)

    return df

def clean_mcp(df): # clean match charting project pages

    df.columns = [clean_column_name(col) for col in df.columns]

    for col in df.columns: 
        if col == "match":
            df[col] = df[col].astype(str).str.replace(r"\s+", "_", regex=True).str.lower().str.strip()

        elif col == "result":
            df[col] = df[col].astype(str).apply(clean_match_results)
        
        else:
            if df[col].astype(str).str.contains('%').any():
                df[col] = pd.to_numeric(df[col].replace("%", "", regex=True), errors="coerce")/100

    df = df.replace(r'^\s*$|^–$|^-$|^0/0$', pd.NA, regex=True)

    return df

def clean_h2h(df): # clean head to head pages

    df.columns = [clean_column_name(col) for col in df.columns]
    df = df.rename(columns={"": "winner"})
    df = df.drop(['more'], axis=1)

    for col in df.columns: 

        if col == "tournament":
            df[col] = df[col].astype(str).str.replace(r"\s+", "_", regex=True).str.lower().str.strip()

        elif col == "time":
            df['total_minutes'] = df['time'].apply(
                lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1])
                if pd.notna(x) and ':' in x and all(part.isdigit() for part in x.split(':')) # deals with empty values
                else pd.NA)

        elif col == "winner":
            df[col] = df[col].apply(clean_winner)

        elif col == "date":
            df[col] = df[col].str.replace(r"[‐‑‒–—−]", "-", regex=True)
            df[col] = pd.to_datetime(df[col], format='%d-%b-%Y')
            df[col] = df[col].dt.strftime('%Y-%m-%d')

        elif col == "surface" or col == "rd":
            df[col] = df[col].str.lower()

        elif col == "score":
            df[col] = df[col].astype(str).apply(lambda x: re.sub(r'\([^)]*\)', '', x) if pd.notna(x) else x)

            def parse_score(s):
                if pd.isna(s) or s.strip().lower() in ['null', 'nan', '']:
                    return pd.Series([0, 0, 0])

                sets = s.strip().split()
                winner_total = 0
                loser_total = 0

                for set_score in sets:
                    games = re.findall(r'\d+', set_score) # x-y, x values are always the winner's
                    if len(games) == 2:
                        winner_total += int(games[0])
                        loser_total += int(games[1])

                return pd.Series([len(sets), winner_total, loser_total])
            df[['num_sets', 'games_won_by_winner', 'games_won_by_loser']] = df[col].apply(parse_score)

        else:
            if df[col].astype(str).str.contains('%').any():
                df[col] = pd.to_numeric(df[col].replace("%", "", regex=True), errors="coerce")/100

    df = df.replace(r'^\s*$|^–$|^-$|^0/0$', pd.NA, regex=True)
    df = df[~((df.isna()) | (df == 0)).all(axis=1)]

    return df