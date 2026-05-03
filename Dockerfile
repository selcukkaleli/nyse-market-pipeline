FROM apache/airflow:2.10.5
RUN pip install --no-cache-dir kaggle boto3 apache-airflow-providers-docker
RUN python -m venv /home/airflow/dbt-venv && \
    /home/airflow/dbt-venv/bin/pip install dbt-athena-community