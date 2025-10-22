from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_gdelt_scraper():
    subprocess.run(["python", "/opt/airflow/dags/scripts/gdelt_scraper.py"], check=True)

default_args = {
    'owner': 'Mohamed',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gdelt_scraper_dag',
    default_args=default_args,
    description='Extract bilingual data from GDELT every 30 minutes',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2025, 10, 21),
    catchup=False,
) as dag:

    run_scraper = PythonOperator(
        task_id='run_gdelt_scraper',
        python_callable=run_gdelt_scraper,
    )
