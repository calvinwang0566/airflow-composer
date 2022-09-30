import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pprint import pprint
from google.cloud import bigquery

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator  import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator


url = 'https://lancomebc.lorealluxe.com.tw'
get_token_url = '/api/Beacon/getToken'
get_trigger_record_url = '/api/Beacon/getTriggerRecord'

date_yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'schedule_interval': '@daily',
    'email': ['calvin.wang@loreal.com'],
    'email_on_failure': True
}


def request_post(url, headers=None, json=None):
    response = requests.post(url, headers=headers, json=json)
    response_status = response.status_code
    response_json = response.json()
    print("Status Code: ", response_status)
    print("JSON Response: ")
    pprint(response_json)    

    return response_status, response_json


def check_request_status(url, headers=None, json=None):
    print("Check if response status is correct.")
    response_status, _ = request_post(url, headers, json)
    if response_status == 200:
        print("Response status correct!")
        return 'yes_generate_signature'
    else:
        print("Response status wrong!")
        return 'no_pass'


def generate_signature(url, headers=None, json=None, **context):
    _, request_json = request_post(url, headers, json)
    signature = request_json['data'][0]['token']

    print(f'Signature: {signature}')

    return signature


def get_pagination(url, headers=None, json=None, **context):
    headers['X-Coolbe-Signature'] = context['task_instance'].xcom_pull(task_ids='yes_generate_signature')
    _, request_json = request_post(url, headers, json)
    page_count = request_json['data'][0]['pagination']['page_count']

    print(f'Page count: {page_count}')

    return page_count


def get_trigger_record_to_csv(url, headers=None, json=None, **context):
    headers['X-Coolbe-Signature'] = context['task_instance'].xcom_pull(task_ids='yes_generate_signature')
    page_count = context['task_instance'].xcom_pull(task_ids='get_pagination')

    data_list = []
    for page in range(page_count):
        json['page'] = page + 1
        _, request_json = request_post(url, headers, json)
    
        request_dataframe = pd.json_normalize(request_json['data'][0]['records'])
        data_list.append(request_dataframe)

    data = pd.concat(data_list, axis=0)
    csv_file_path = 'LAN_beacon_get_trigger.csv'
    data.to_csv(csv_file_path, index=False, encoding='utf_8_sig')



def data_to_gcs(**context):
    gcs_hook = GoogleCloudStorageHook(gcp_conn_id='bigquery_default')
    gcs_hook.upload(
            bucket_name='airflow_data_lake', 
            object_name='LAN_beacon_get_trigger.csv', 
            filename='LAN_beacon_get_trigger.csv'
        )


schema = [
    {"mode": "NULLABLE",
     "name": "event_name",
     "type": "STRING"},
    {"mode": "NULLABLE",
     "name": "event_start_at",
     "type": "DATE"},
    {"mode": "NULLABLE",
     "name": "event_end_at",
     "type": "DATE"},
    {"mode": "NULLABLE",
     "name": "receiver_uid",
     "type": "STRING"},
    {"mode": "NULLABLE",
     "name": "event_type",
     "type": "STRING"},
    {"mode": "NULLABLE",
     "name": "event_time",
     "type": "TIMESTAMP"},
    {"mode": "NULLABLE",
     "name": "first_follow",
    "type": "TIMESTAMP"},
    {"mode": "NULLABLE",
     "name": "bind_date",
     "type": "STRING"},
    {"mode": "NULLABLE",
     "name": "brand_id",
     "type": "STRING"}
     ]

print(schema)

with DAG(dag_id='LAN_beacon_api_data_connection', 
         default_args=default_args,
         schedule_interval='@daily',
         start_date=days_ago(1)) as dag:

    check_request_status_flow = BranchPythonOperator(
        task_id = 'check_request_status',
        python_callable = check_request_status,
        op_kwargs={'url': url+get_token_url,
                   'headers': {"Content-Type": "application/json"},
                   'json': {"secret": "b99a7c32f9ce7d6a8fd3353499dc3759"}}
    )

    generate_signature_flow = PythonOperator(
        task_id = 'yes_generate_signature',
        python_callable = generate_signature,
        op_kwargs = {'url': url+get_token_url,
                     'headers': {"Content-Type": "application/json"},
                     'json': {"secret": "b99a7c32f9ce7d6a8fd3353499dc3759"}}
    )

    get_pagination_flow = PythonOperator(
        task_id = 'get_pagination',
        python_callable = get_pagination,
        op_kwargs = {'url': url+get_trigger_record_url,
                     'headers': {"Content-Type": "application/json"},
                     'json': {"page": "1",
                              "page_size": "1000",
                              "date_start_at": date_yesterday,
                              "date_end_at": date_yesterday}}
    )

    get_trigger_record_to_csv_flow = PythonOperator(
        task_id = 'get_trigger_record_to_csv',
        python_callable = get_trigger_record_to_csv,
        op_kwargs = {'url': url+get_trigger_record_url,
                     'headers': {"Content-Type": "application/json"},
                     'json': {"page": "1",
                              "page_size": "1000",
                              "date_start_at": date_yesterday,
                              "date_end_at": date_yesterday}}
    )

    data_to_gcs_flow = PythonOperator(
        task_id = "data_to_gcs",
        python_callable = data_to_gcs
    )

    gcs_to_bq_flow = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        bucket = 'airflow_data_lake',
        source_objects = ['LAN_beacon_get_trigger.csv'],
        source_format = 'CSV',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        schema_fields = schema,
        destination_project_dataset_table = 'marts.beacon_api',
        write_disposition = 'WRITE_APPEND',
        bigquery_conn_id = 'bigquery_default'
    )

    do_nothing = DummyOperator(task_id='no_pass')


check_request_status_flow >> [generate_signature_flow, do_nothing]

generate_signature_flow >> get_pagination_flow >> get_trigger_record_to_csv_flow >> data_to_gcs_flow >> gcs_to_bq_flow