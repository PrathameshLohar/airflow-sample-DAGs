from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Configuration
LOCAL_FILE_PATH = "/path/to/your/local/file.csv"
S3_BUCKET = "your-s3-bucket-name"
S3_KEY = "redshift_data/file.csv"
REDSHIFT_TABLE = "your_redshift_table"
IAM_ROLE_ARN = "arn:aws:iam::1234567890:role/your-redshift-role"

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

dag = DAG(
    'upload_local_to_redshift',
    default_args=default_args,
    schedule_interval=None,
    description='Upload data from local to S3 and then to Redshift'
)

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename=LOCAL_FILE_PATH,
        key=S3_KEY,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Uploaded {LOCAL_FILE_PATH} to s3://{S3_BUCKET}/{S3_KEY}")

def copy_to_redshift():
    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    copy_query = f"""
        COPY {REDSHIFT_TABLE}
        FROM 's3://{S3_BUCKET}/{S3_KEY}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        CSV
        IGNOREHEADER 1;
    """
    redshift_hook.run(copy_query)
    print("Data copied to Redshift")

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

copy_task = PythonOperator(
    task_id='copy_to_redshift',
    python_callable=copy_to_redshift,
    dag=dag
)

upload_task >> copy_task
