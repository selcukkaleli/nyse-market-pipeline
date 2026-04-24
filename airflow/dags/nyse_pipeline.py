from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_source_data():
    print("Source data ready")

def start_pipeline():
    print("Pipeline started")

with DAG(
    dag_id="nyse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval = None,
    catchup= False) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=check_source_data
    )
    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=start_pipeline
    )

    task_1 >> task_2