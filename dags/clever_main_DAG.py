from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.bronze.main import execute_bronze_layer
from scripts.silver.main import process_bronze_to_silver
from scripts.gold.main import process_silver_to_gold
from scripts.database_operations.main import main as upload_to_postgres

# Define default arguments
default_args = {
    "owner": "alec.ventura, ricardo.motta", 
    "start_date": datetime(2024, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the main DAG
with DAG(
    "clever_main_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval='20 0 * * *',  # Runs daily at 00:20
    max_active_runs=1
) as dag:

    # Start task
    start_task = EmptyOperator(task_id='Start')

    # Task to execute the BRONZE layer
    save_to_bronze_task = PythonOperator(
        task_id='save_raw_files_to_bronze',
        python_callable=execute_bronze_layer,
        execution_timeout=timedelta(minutes=10)
    )

    # Task to process the SILVER layer
    process_silver_task = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=process_bronze_to_silver,
        op_kwargs={
            'bronze_dir': '/opt/airflow/data/BRONZE',
            'silver_dir': '/opt/airflow/data/SILVER'
        },
        execution_timeout=timedelta(minutes=10)
    )

    # Task to process the GOLD layer
    process_gold_task = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=process_silver_to_gold,
        op_kwargs={
            'silver_dir': '/opt/airflow/data/SILVER',
            'gold_dir': '/opt/airflow/data/GOLD'
        },
        execution_timeout=timedelta(minutes=10)
    )

    # Task to upload data to Postgres
    upload_to_postgres_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        execution_timeout=timedelta(minutes=10)
    )

    # Finish task
    finish_task = EmptyOperator(task_id='Finish')

    # Define task relationships
    start_task >> save_to_bronze_task >> process_silver_task >> process_gold_task >> upload_to_postgres_task >> finish_task
