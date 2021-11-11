import json
import urllib.parse
import boto3
import os
import urllib3
from http.client import responses

s3 = boto3.client('s3')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    trifacta_auth_token = 'update your auth token'
    trifacta_wrangled_dataset_id = 'update the wrangled_dataset_id'
    print('Run Trifacta job on new file: {}'.format(key))
    trifacta_runjob_endpoint = 'https://yourworkspace.cloud.trifacta.com/v4/jobGroups'
    trifacta_job_param = {
        "wrangledDataset": {"id": trifacta_wrangled_dataset_id},
        "runParameters": {"overrides": {"data": [{"key": "filename","value": key}]}}
    }
    print('Run Trifacta job param: {}' .format(trifacta_job_param))
    trifacta_headers = {
        "Content-Type":"application/json",
        "Authorization": "Bearer "+trifacta_auth_token
    }
    http = urllib3.PoolManager()
    r = http.request('POST',trifacta_runjob_endpoint, headers=trifacta_headers,  body=json.dumps(trifacta_job_param))
    print('Status Code : {}'.format(r.status))
    print('Result : {}'.format(responses[r.status]))
    return 'End File event'.format(key)