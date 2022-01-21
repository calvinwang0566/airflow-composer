from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

secret_volume = Secret(
    'volume',
    # Path where we mount the secret as volume
    '/var/secrets/google',
    # Name of Kubernetes Secret
    'service-account',
    # Key in the form of service account file name
    'service-account.json'
)

with DAG(
    dag_id='Ads_and_Promotion_flow',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    run_dbt = KubernetesPodOperator(
        task_id='run_dbt',
        name="Ads_and_Promotion_flow",
        namespace='default',
        image_pull_policy='Always',
        # image='fishtownanalytics/dbt:1.0.0',
        image='asia.gcr.io/loreal-tw/dbt-image:latest',
        secrets=[secret_volume],
        env_vars={'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/service-account.json'},
        get_logs=True,
        cmds=["/bin/bash", "-c"],
        arguments=[
            """
            dbt run --profiles-dir /dbt --models ga_daily_report --target loreal_bq --fail-fast; ret=$?; exit $ret
            """
        ],
        is_delete_operator_pod=True,
    )

    run_dbt