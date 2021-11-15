# Library imports
from public_spotify_ETL import run_spotify_etl
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Set DAG arguments
default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': timedelta(2021, 10, 10),
    'retries':1,
    'retry_delay': timedelta(minutes=2)
}

# DAG Definition
dag = DAG(
    'spotify_ETL',
    description='Very first Spotify ETL',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# Task Definition
task1 = PythonOperator(
    task_id='song_to_Db',
    python_callable=run_spotify_etl,
    dag=dag
)

# Task Pipeline
task1
