from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('ingestion_pipeline_dag',
         start_date=datetime(2021, 2, 15),
         schedule_interval='@once',
         default_args=default_args,
         # template_searchpath='/usr/local/airflow/include',
         catchup=False
         ) as dag:

    load_dictionaries = BashOperator(
        task_id='load_dictionaries',
        bash_command='python3 ../load_dictionaries.py ',
        dag=dag,
        params={'csv_path_taxi_zone': '/path/to/lookup_table/'},
    )

    load_taxi_trips = BashOperator(
        task_id='load_taxi_trips',
        bash_command='python3 ../load_taxi_trips.py',
        dag=dag,
        params={'csv_path_taxi_trips': '/path/to/trips_csv_files/'}
    )

    load_dictionaries >> load_taxi_trips





