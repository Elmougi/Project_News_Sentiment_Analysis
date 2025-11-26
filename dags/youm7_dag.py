from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_youm7_scraper():
    subprocess.run(["python", "/opt/airflow/dags/scripts/youm7_scraper.py"], check=True)

default_args = {
    'owner': 'Mohamed',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='youm7_scraper_dag',
    default_args=default_args,
    description='Scrape Youm7 news every 30 minutes',
    schedule_interval='*/30 * * * *',  # كل نص ساعة
    start_date=datetime(2025, 10, 21),
    catchup=False,
) as dag:

    run_scraper = PythonOperator(
        task_id='run_youm7_scraper',
        python_callable=run_youm7_scraper,
    )
