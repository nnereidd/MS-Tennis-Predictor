from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
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
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    tags=['tennis', 'lambda'],
)


