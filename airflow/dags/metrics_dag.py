from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('metrics_dag',
         start_date=datetime(2021, 2, 15),
         schedule_interval='@once',
         default_args=default_args,
         template_searchpath='/usr/local/airflow/include',
         catchup=False
         ) as dag:

    run_first_task = PostgresOperator(
        task_id='task_2_1',
        postgres_conn_id='postgres',
        sql='task_2_1.sql',
        params={'k': '50', 'p_date': '2019-01'}
    )
    run_second_task = PostgresOperator(
        task_id='task_2_2',
        postgres_conn_id='postgres',
        sql='task_2_2.sql'
    )

    run_first_task >> run_second_task
