from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, col
from pyspark.sql.functions import regexp_extract, regexp_replace
from datetime import datetime
import io
import boto3
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# set up log buffer
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_buffer = io.StringIO()
s3 = boto3.client("s3")
log_s3_path = f"logs/processed/player_statistics/{timestamp}.log"

# load parquet files from folder
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://tennis-predictor-data/raw/player_statistics/"],
        "recurse": True
    },
    transformation_ctx="raw_df"
)

df = raw_df.toDF()
df = df.withColumn("source_file", input_file_name())