from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import json
import boto3
import logging
from botocore.config import Config
import time

# DAG default settings
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='tennis_pipeline_master',
    default_args=default_args,
    description='Full pipeline: scrape + clean with dynamic batching',
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    tags=['tennis', 'lambda'],
)

# Config
S3_BUCKET = 'tennis-predictor-data'
PLAYER_LIST_KEY = 'raw/rankings/player_list.json'

BATCH_SIZES = {
    'scrape_player_statistics_lambda': 8,
    'scrape_match_charting_project_lambda': 10,
    'scrape_h2h_lambda': 1
}

# fetch player count and calculate batch sizes 
@task()
def fetch_batch_counts():
    session = boto3.Session(profile_name="dar")
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=PLAYER_LIST_KEY)
    players = json.loads(obj['Body'].read().decode('utf-8'))
    player_count = len(players)

    batch_info = {}
    for scraper, batch_size in BATCH_SIZES.items():
        num_batches = (player_count + batch_size - 1) // batch_size
        batch_info[scraper] = num_batches
    return batch_info

@task()
def generate_batches(scraper_name, batch_info):
    return [{'scraper': scraper_name, 'batch': i} for i in range(batch_info[scraper_name])]

# call scraping with dynamic batches
@task()
def _run_scraper_lambda(batch):
    logging.info(f"Invoking Lambda for {batch['scraper']} with batch {batch['batch']}")
    time.sleep(1)

    # Ensures boto3 waits for result
    config = Config(
    read_timeout=300,       # 5 minutes max wait
    connect_timeout=30,     
    retries={'max_attempts': 2})

    session = boto3.Session(profile_name="dar")
    client = session.client('lambda', config=config)
    response = client.invoke(
        FunctionName=batch['scraper'],
        Payload=json.dumps({"batch": batch['batch']}).encode('utf-8')
    )
    logging.info(f"Response: {response['StatusCode']}")

run_scraper_lambda = _run_scraper_lambda

scrape_rankings = LambdaInvokeFunctionOperator(
    task_id='scrape_rankings_lambda',
    function_name='scrape_rankings_lambda',
    log_type='Tail',
    aws_conn_id='aws_dar',
    dag=dag
)

cleaning_functions = [
    'clean_rankings_lambda',
    'clean_player_statistics_lambda',
    'clean_match_charting_project_lambda',
    'clean_h2h_lambda'
]

cleaning_tasks = []
for cleaner in cleaning_functions:
    task = LambdaInvokeFunctionOperator(
        task_id=cleaner,
        function_name=cleaner,
        log_type='Tail',
        aws_conn_id='aws_dar',
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    cleaning_tasks.append(task)

# DAG orchestration
with dag:
    batch_info = fetch_batch_counts()
    batch_info >> scrape_rankings >> cleaning_tasks[0] 

    stats_batches = generate_batches.override(task_id="generate_stats_batches")( 'scrape_player_statistics_lambda', batch_info)
    match_batches = generate_batches.override(task_id="generate_match_batches")( 'scrape_match_charting_project_lambda', batch_info)

    stats_tasks = run_scraper_lambda.override(task_id="stats_lambda_runner", max_active_tis_per_dag=5).expand(batch=stats_batches)
    match_tasks = run_scraper_lambda.override(task_id="match_lambda_runner", max_active_tis_per_dag=3).expand(batch=match_batches)

    h2h_batches = generate_batches.override(task_id="generate_h2h_batches")( 'scrape_h2h_lambda', batch_info)
    h2h_tasks = run_scraper_lambda.override(task_id="h2h_lambda_runner", max_active_tis_per_dag=6).expand(batch=h2h_batches)

    # flow is scrape ps, mcp then scrape h2h then all 4 cleaners
    batch_info >> stats_batches >> stats_tasks >> match_batches >> match_tasks >> h2h_batches >> h2h_tasks >> cleaning_tasks