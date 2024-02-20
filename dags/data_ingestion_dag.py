import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scrapper import scrape_data_callable
from upload_postgres import ingest_callable

# Connect to the database
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

URL    = 'https://www.premierleague.com/tables'

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y%m%d\') }}.csv'
TABLE_NAME_TEMPLATE  = 'league_table_{{ execution_date.strftime(\'%Y%m%d\') }}'


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

workflow = DAG(
    dag_id='LocalIngestionDAGv2',
    start_date=datetime(2024, 2, 8),
    schedule_interval="0 11 * * 6-7",
    default_args=default_args,
    catchup=True
)

with workflow as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data_callable,
        op_kwargs={'url': URL, 'save_path': OUTPUT_FILE_TEMPLATE}
    )

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_callable,
        op_kwargs={
            'db_user': DB_USER, 
            'db_pass': DB_PASS, 
            'db_host': DB_HOST, 
            'db_port': DB_PORT, 
            'db_name': DB_NAME, 
            'table_name': TABLE_NAME_TEMPLATE,
            'data_filepath': OUTPUT_FILE_TEMPLATE
        }
    )

    scrape_task >> ingest_task