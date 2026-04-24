from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

DOWNLOAD_PATH = "/tmp/nyse_data"

def download_data():

    import kaggle

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('dgawlik/nyse', path= DOWNLOAD_PATH, unzip=True)

def upload_to_s3():

    import boto3
    import os

    s3_client = boto3.client('s3')
    
    for file_name in os.listdir(DOWNLOAD_PATH):
        file_path = os.path.join(DOWNLOAD_PATH, file_name)
        s3_client.upload_file(file_path, 'nyse-market-pipeline-raw-data', file_name)
        print(f"Uploaded {file_name} to S3")    

def run_spark():
    pass

def run_dbt():
    pass

with DAG(
    dag_id="nyse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval = None,
    catchup= False) as dag:

    download_data_task = PythonOperator(
        task_id="download_data",
        python_callable=download_data
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    run_spark_task = PythonOperator(
        task_id="run_spark",
        python_callable=run_spark
    )

    run_dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt
    )


    download_data_task >> upload_to_s3_task >> run_spark_task >> run_dbt_task
