from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    t1 = BashOperator(
        task_id='t1',
        bash_command='echo Hello World',
    )

    t1