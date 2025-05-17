from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_postgres_to_duckdb',
    default_args=default_args,
    description='Extract metrics from Postgres and load into DuckDB',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command='python /opt/airflow/etl/etl_postgres_to_duckdb.py'
    )