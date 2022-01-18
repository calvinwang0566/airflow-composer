from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    t1 = BigQueryOperator(
        task_id='t1',
        sql=
        """
            SELECT
                "Success" AS col1 
        """,
        destination_dataset_table='wilson_test.airflow_testing',
        bigquery_conn_id='bigquery_default',
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False
    )

    t1