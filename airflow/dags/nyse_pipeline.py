from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from airflow.providers.docker.operators.docker import DockerOperator
import kaggle
import boto3
import os
from docker.types import Mount

DOWNLOAD_PATH = "/tmp/nyse_data"

def download_data():

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('dgawlik/nyse', path= DOWNLOAD_PATH, unzip=True)

def upload_to_s3():

    s3_client = boto3.client('s3')
    
    for file_name in os.listdir(DOWNLOAD_PATH):
        file_path = os.path.join(DOWNLOAD_PATH, file_name)
        s3_client.upload_file(file_path, 'nyse-market-pipeline-raw-data', file_name)
        print(f"Uploaded {file_name} to S3")    

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

    run_spark_task = DockerOperator(
        task_id="run_spark",
        image="nyse-market-pipeline-spark:latest",
        entrypoint="/opt/spark/bin/spark-submit",
        command="/opt/spark/work/stocks_enriched.py",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/Users/selcukkaleli/nyse-market-pipeline/spark", 
                target="/opt/spark/work", 
                type="bind"),
            Mount(source="/Users/selcukkaleli/.aws", 
                target="/root/.aws", 
                type="bind",
                read_only=True),
        ],
        environment={
            "SPARK_DRIVER_MEMORY": "4g",
            "SPARK_EXECUTOR_MEMORY": "4g"
        },
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        dag=dag
    )

    run_dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt
    )


    download_data_task >> upload_to_s3_task >> run_spark_task >> run_dbt_task
