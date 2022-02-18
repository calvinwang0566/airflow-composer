from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
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
    dag_id='google_trend',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
) as dag:

    def pytrends_func():
        from pytrends.request import TrendReq
        import pandas as pd
        import pytz
        from datetime import datetime
        from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

        timezone_tp = pytz.timezone('Asia/Taipei')

        gtrend_keyword_list = [
            {"keyword":"蘭蔻","is_luxe":True },
            {"keyword":"Lancome","is_luxe":True},
            {"keyword":"契爾氏","is_luxe":True},
            {"keyword":"Kiehl's","is_luxe":True},
            {"keyword":"植村秀","is_luxe":True},
            {"keyword":"shu uemura","is_luxe":True},
            {"keyword":"聖羅蘭","is_luxe":True},
            {"keyword":"YSL","is_luxe":True},
            {"keyword":"亞曼尼","is_luxe":True},
            {"keyword":"Armani","is_luxe":True},
            {"keyword":"碧兒泉","is_luxe":True},
            {"keyword":"Biotherm","is_luxe":True},
            {"keyword":"赫蓮娜","is_luxe":True},
            {"keyword":"Helena Rubinstein","is_luxe":True},
            {"keyword":"雅詩蘭黛","is_luxe":False},
            {"keyword":"Chanel","is_luxe":False},
            {"keyword":"Dior","is_luxe":False},
            {"keyword":"Aesop","is_luxe":False},
            {"keyword":"歐舒丹","is_luxe":False},
            {"keyword":"嬌蘭","is_luxe":False},
            {"keyword":"Jo malone","is_luxe":False},
            {"keyword":"TOM FORD","is_luxe":False},
            {"keyword":"LA MER","is_luxe":False},
            {"keyword":"海洋拉娜","is_luxe":False},
            {"keyword":"La Prairie","is_luxe":False},
            {"keyword":"肌膚之鑰","is_luxe":False},
            {"keyword":"Clé de Peau Beauté","is_luxe":False},
            {"keyword":"MAC","is_luxe":False},
            {"keyword":"Make Up forever","is_luxe":False},
            {"keyword":"Bobbi brown","is_luxe":False},
        ]

        pytrend = TrendReq(hl='en-US', tz=0)

        result = pd.DataFrame()
        for gtrend_keyword in gtrend_keyword_list:
            keyword = gtrend_keyword['keyword']
            is_luxe = gtrend_keyword['is_luxe']
            kw_list = []
            kw_list.append(keyword)
            pytrend.build_payload(
                kw_list=kw_list,
                cat=0,
                timeframe='today 5-y',
                geo='TW'
            )
            data = pytrend.interest_over_time()
            data.drop('isPartial', 1, inplace=True)
            data.set_axis(['hits'], axis=1, inplace=True)
            data['keyword'] = keyword
            data['is_luxe'] = is_luxe
            result = result.append(data)

        result['updated_datetime'] = datetime.now(timezone_tp)
        result.to_csv('gtrend.csv', encoding='utf_8_sig')

        # print(data)

        # from google.cloud import bigquery
        gcs_hook = GoogleCloudStorageHook(gcp_conn_id='bigquery_default')
        gcs_hook.upload(
            bucket_name='airflow_data_lake', 
            object_name='gtrend.csv', 
            filename='gtrend.csv'
        )

    get_data = PythonVirtualenvOperator(
        task_id='get_data',
        requirements=['pytrends'],
        python_callable= pytrends_func
    )

    schema = [
        {
            "mode": "NULLABLE",
            "name": "date",
            "type": "DATE"
        },
        {
            "mode": "NULLABLE",
            "name": "hits",
            "type": "INTEGER"
        },
        {
            "mode": "NULLABLE",
            "name": "keyword",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "is_luxe",
            "type": "BOOL"
        },
        {
            "mode": "NULLABLE",
            "name": "updated_datetime",
            "type": "TIMESTAMP"
        }
    ]

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='airflow_data_lake',
        source_objects=['gtrend.csv'],
        source_format='CSV',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        schema_fields=schema,
        destination_project_dataset_table='marts.gtrend',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default'
    )

    run_dbt = KubernetesPodOperator(
        task_id='run_dbt',
        name="gtrend_flow",
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
            dbt run --profiles-dir /dbt --models gtrend_slope --target loreal_bq --fail-fast; ret=$?; exit $ret
            """
        ],
        is_delete_operator_pod=True,
    )

    get_data >> gcs_to_bq >> run_dbt