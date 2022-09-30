import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pprint import pprint
from google.cloud import bigquery

def request_post(url, headers=None, json=None):
    response = requests.post(url, headers=headers, json=json)
    response_status = response.status_code
    response_json = response.json()
    print("Status Code: ", response_status)
    # print("JSON Response: ")
    # pprint(response_json)    

    return response_status, response_json



print("Check if response status is correct.")
print()
response_status, request_json = request_post(url='https://lancomebc.lorealluxe.com.tw'+'/api/Beacon/getToken', 
                                  headers={"Content-Type": "application/json"}, 
                                  json={"secret": "b99a7c32f9ce7d6a8fd3353499dc3759"})
if response_status == 200:
    print("Response status correct!")
    print()
    
    signature = request_json['data'][0]['token']
    print(f'Signature: {signature}')
    print()

    _, request_json = request_post(url='https://lancomebc.lorealluxe.com.tw'+'/api/Beacon/getTriggerRecord', 
                                   headers={"Content-Type": "application/json",
                                            "X-Coolbe-Signature": signature}, 
                                   json={"page": "1",
                                         "page_size": "1000",
                                         "date_start_at": "2022-09-20",
                                         "date_end_at": "2022-09-20"})

    page_count = request_json['data'][0]['pagination']['page_count']
    print(f'Total pages: {page_count}')

    data_list = []
    for page in range(page_count):
        print(f'Page: {str(page+1)}')
        _, request_json = request_post(url='https://lancomebc.lorealluxe.com.tw'+'/api/Beacon/getTriggerRecord',
                                       headers={"Content-Type": "application/json",
                                                "X-Coolbe-Signature": signature}, 
                                       json={"page": str(page+1),
                                             "page_size": "1000",
                                             "date_start_at": "2022-09-20",
                                             "date_end_at": "2022-09-20"})
        try:
            request_dataframe = pd.json_normalize(request_json['data'][0]['records'])
            data_list.append(request_dataframe)
        except:
            print(request_json)

    data = pd.concat(data_list, axis=0)
    csv_file_path = 'beacon_get_trigger.csv'
    data.to_csv(csv_file_path, index=False, encoding='utf_8_sig')


else:
    sys.exit("Response status wrong!")
    