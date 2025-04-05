from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import json
import boto3

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
    schedule_interval='@biweekly',
    start_date=days_ago(1),
    catchup=False,
    tags=['tennis', 'lambda'],
)

# dynamic batching (players in the json file divided by batch_size)
def fetch_batch_counts(**context):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=PLAYER_LIST_KEY)
    players = json.loads(obj['Body'].read().decode('utf-8'))
    player_count = len(players)

    batch_info = {}
    for scraper, batch_size in BATCH_SIZES.items():
        num_batches = (player_count + batch_size - 1) // batch_size
        batch_info[scraper] = num_batches

    context['ti'].xcom_push(key='batch_info', value=batch_info)

S3_BUCKET = 'tennis-predictor-data'
PLAYER_LIST_KEY = 'raw/rankings/player_list.json'

BATCH_SIZES = {
    'scrape_player_statistics': 20,
    'scrape_match_charting_project': 20,
    'scrape_h2h': 1
}

MAX_PARALLELISM = 10
