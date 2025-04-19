# Tennis ATP Men's Singles Predictor (Data Pipeline and Dashboard)

This project is part of a larger goal to predict match outcomes in men's singles tennis using machine learning. Here, the focus is on building a **fully automated data pipeline** and **interactive dashboard** that tracks and visualizes the latest stats for the **top 15 ATP players**. 

---

## Project Features

- **Automated Scraping**: Every month, the pipeline scrapes updated stats for the top 15 ATP players directly from TennisAbstract website.
- **Cloud Infrastructure**: Built on AWS using:
  - **Lambda** using docker, for scraping and cleaning
  - **S3** for cloud storage (raw, logs, textlogs and processed data)
  - **Apache Airflow (MWAA)** for orchestration and parallel processing
- **Snowflake**: Cloud-based data warehouse that ingests the data automatically allowing SQL queries
- **Dynamic Dashboard**: A visual dashboard that automatically updates every month with the latest player performance metrics.
- **Data Format**: All data is saved in efficient `.parquet` format, structured by player and metric type.

---

## How It Works

This project uses a fully automated, serverless cloud pipeline to gather, process, and store statistical data on the top 15 ATP men's singles players. The goal is to ensure that the data — which powers future visualizations and machine learning — is always fresh and synchronized with the Tennis Abstract website: https://www.tennisabstract.com/.

### 1. Scraper Lambda Functions

Every month, **Airflow (MWAA)** triggers four scraping Lambda functions, each responsible for a different type of player data:

- `scrape_rankings` — fetches the current top 15 ATP rankings *(no batching required)*
- `scrape_player_statistics` — general stats *(batched in groups of 10 players)*
- `scrape_match_charting_project` — advanced stats from the charting project *(batched in groups of 10 players)*
- `scrape_h2h` — head-to-head match data *(batched per player)*

Each function uses Selenium with headless Chrome (inside Docker) to scrape dynamic website content and stores the output in `.parquet` format for S3.

### 2. Cleaning Lambda Functions 

Once all scraping tasks are completed, Airflow automatically triggers four corresponding cleaning Lambda functions:

- `clean_rankings`
- `clean_player_statistics`
- `clean_match_charting_project`
- `clean_h2h`

Each cleaner reads the raw `.parquet` files from S3, processes and transforms the data (e.g., removing missing values, renaming fields, joining data), then writes cleaned outputs to S3.

### 3. Airflow DAG Orchestration

The pipeline is orchestrated using a single master DAG: `dag.py`, which:

- Runs automatically every month (`@monthly`)
- Dynamically reads the number of players from a json file generated by the scripts in S3
- Calculates the number of scraping batches required
- Executes all scraper tasks in parallel using TaskGroups
- Waits until all scrapers finish before triggering cleaning
- Runs all cleaner tasks sequentially to reduce concurrency costs

### 4. Snowflake Integration 

To support querying, analytics, and BI integrations, the pipeline connects to **Snowflake**, which:

- **Snowpipe** is used to **automatically ingest cleaned `.parquet` files** from the `processed/` S3 folder into Snowflake
- Once ingested, cleaned data is available as **queryable Snowflake tables**
- Used for SQL analysis, visualization in Power BI, and ultimately feeding machine learning models


### 5. Dashboard

---

### Cloud-Native, Serverless and Automated

- **Built for scale**: the system can support scraping any number of ATP players by simply updating scrape_rankings.py code
- **Zero human intervention**: no manual scripts or cronjobs since everything runs autonomously in the cloud
- **Logs + Archiving**: S3 automatically versions raw and processed files for traceability
- **Modular Lambda Functions**: Each script tasks is isolated in its own Lambda function, making the system easy to test, debug, and extend
- **Batching + Parallelization**: Airflow dynamically calculates batch requirements based on player list size and runs scrapers in parallel which optimizes execution time and resource usage.
