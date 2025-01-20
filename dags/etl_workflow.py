# dags/etl_workflow.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import sys

# Import the necessary functions for ETL tasks
from scripts.extract import extract_data
from scripts.transform import (
    process_price_data,
    process_borrower_data,
    process_fico_data,
)
from scripts.load import load_to_database

# Set up logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline DAG',
    schedule_interval=None,  # Can be changed to a schedule (e.g., '0 0 * * *' for daily)
    catchup=False,
) as dag:

    # Extract task
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_args=['data/Nat_Gas.csv', 'data/borrower_data.csv', 'data/fico_scores.csv'],
        dag=dag,
    )

    # Transform tasks
    transform_price_task = PythonOperator(
        task_id='transform_price_data',
        python_callable=process_price_data,
        op_args=['data/Nat_Gas.csv'],
        dag=dag,
    )

    transform_borrower_task = PythonOperator(
        task_id='transform_borrower_data',
        python_callable=process_borrower_data,
        op_args=['data/borrower_data.csv'],
        dag=dag,
    )

    transform_fico_task = PythonOperator(
        task_id='transform_fico_data',
        python_callable=process_fico_data,
        op_args=['data/fico_scores.csv'],
        dag=dag,
    )

    # Load task
    load_task = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_to_database,
        op_args=['outputs/etl_output.db'],
        dag=dag,
    )

    # Define task dependencies (order of execution)
    extract_task >> [transform_price_task, transform_borrower_task, transform_fico_task] >> load_task
