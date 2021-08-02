from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


from datetime import datetime
import sys
import requests
import json
import os
import logging
import time
import configparser

from sql_query.airtable_query import (
    create_airtable_web_events,
    copy_all_web_sql,
    create_airtable_app_events,
    copy_all_app_sql,
    raw_event_table_value,
    event_sequence,
    event_metrics,
    attribution_table,
    create_schema
)

# store data to s3
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

# various of settings
config = configparser.ConfigParser()
config.read('/usr/local/airflow/setting.conf')
os.environ["AWS_ACCESS_KEY_ID"] =  config['DEFAULT']["AWS_ACCESS_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config['DEFAULT']["AWS_SECRET_KEY"]
os.environ["AWS_DEFAULT_REGION"] = config['DEFAULT']["AWS_DEFAULT_REGION"]
warehouse_host = config['DEFAULT']["WAREHOUSE_HOST"]
warehouse_account = config['DEFAULT']["WHAREHOUSE_ACCOUNT"]
warehouse_password = config['DEFAULT']["WAREHOUSE_PASSWORD"]
api_key = "Bearer {}".format(config["DEFAULT"]["AIRTABLE_KEY"])


# call restful api
def get_request(**kwargs):
    url = kwargs["url"]
    local_path = kwargs["local_path"] #  /usr/local/airflow/temp
    headers = {'Authorization': api_key}
    stamp = 0
    offset = None
    
    try:
        logging.info(f"Calling RESTFUL endpoint : {url}")
        r = requests.get(url, headers=headers)
        assert r.status_code == 200, "Not 200..."
        raw = r.json()
        data = raw["records"]
        logging.info("The count of the data {}".format(len(data)))
        file_name = f"output_path_{stamp}.jsonl"
        save_file_path = os.path.join(local_path, file_name)
        with open(save_file_path, 'w') as outfile:
            for entry in data:
                f_entry = {k.lower(): v for k, v in entry["fields"].items()}
                if 'metadata' in f_entry:
                    f_entry['metadata'] = json.loads(f_entry['metadata'])
                if 'event_properties' in f_entry:
                    f_entry['event_properties'] = json.loads(f_entry['event_properties'])
                json.dump(f_entry, outfile)
                outfile.write('\n')
        stamp += 1
    except:
        logging.error("Response is not 200 please check api serive")
        sys.exit(1)

    time.sleep(1) # avoid the limitation

    try:
        offset = raw["offset"] # for the next page
    except:
        logging.info("No more next page")
    
    while True:
        try:
            cond_url = url + "offset={}".format(offset)
            logging.info(f"Calling RESTFUL endpoint : {cond_url}")
            r = requests.get(cond_url.format(offset), headers=headers)
            assert r.status_code == 200, "Not 200..."
            raw = r.json()
            data = raw["records"]
            logging.info("The count of the data {}".format(len(data)))
            file_name = f"output_path_{stamp}.jsonl"
            save_file_path = os.path.join(local_path, file_name)
            with open(save_file_path, 'w') as outfile:
                for entry in data:
                    f_entry = {k.lower(): v for k, v in entry["fields"].items()}
                    if 'metadata' in f_entry:
                        f_entry['metadata'] = json.loads(f_entry['metadata'])
                    if 'event_properties' in f_entry:
                        f_entry['event_properties'] = json.loads(f_entry['event_properties'])
                    json.dump(f_entry, outfile)
                    outfile.write('\n')
            stamp += 1
            time.sleep(1) # avoid the limitation
            try:
                offset = raw["offset"] # for the next page
            except:
                logging.info("No more next page")
                break
        except:
            logging.error("Response is not 200 please check api serive")
            sys.exit(1)
            


def upload_files_to_s3(**kwargs):
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket_name = kwargs['bucket_name']
    local_temp = kwargs['local_temp'] # /usr/local/airflow/temp_app
    key_name = kwargs['key_name'] # app_events or web_events
    for file in os.listdir(local_temp):
        try:
            s3_file_path = os.path.join(key_name, file)
            local_file = os.path.join(local_temp, file)
            response = hook.load_file(local_file, s3_file_path, bucket_name=bucket_name, replace=True)
            logging.info(f"{local_file} upload to {s3_file_path} , path: {s3_file_path}")
        except:
            logging.error("Something went wrong during uploading files of {}/{} to s3".format(key_name, file))
            logging.error(response)
            sys.exit(1)


# write files to datawarehouse
def write_s3_to_warehouse(**kwargs):
    aws_hook = AwsHook("aws_credentials")
    sql_statement= kwargs["sql_statement"]
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statement.format(credentials.access_key, credentials.secret_key))


def clean_up():
    for file in os.listdir("/usr/local/airflow/temp_app"):
        local_file = os.path.join("/usr/local/airflow/temp_app", file)
        print(f"file {local_file} cleaning up...")
        os.remove(local_file)
    for file in os.listdir("/usr/local/airflow/temp_web"):
        local_file = os.path.join("/usr/local/airflow/temp_web", file)
        print(f"file {local_file} cleaning up...")
        os.remove(local_file)


with DAG('airtable_pipeline', description='pipeline_for_airtable_api', schedule_interval='@once', start_date=datetime(2020, 8, 2), catchup=False) as dag:

    api_get_web = PythonOperator(task_id='api_get_request_web', provide_context=True, python_callable=get_request, 
                                 op_kwargs = {
                                    "url" : "https://api.airtable.com/v0/appWzDISwYl2XFcEz/tblOc1PAd1ohZJ7lX?",
                                    "local_path": "/usr/local/airflow/temp_web"})

    api_get_app = PythonOperator(task_id='api_get_request_app', provide_context=True, python_callable=get_request,
                                op_kwargs = {
                                    "url": "https://api.airtable.com/v0/appWzDISwYl2XFcEz/tblszfEdmGU3mQfyR?",
                                    "local_path": "/usr/local/airflow/temp_app"})

    load_files_s3_web = PythonOperator(task_id='load_files_to_s3_web', provide_context=True, python_callable=upload_files_to_s3,
                                op_kwargs = {
                                    "bucket_name": "luko-data-eng-exercice",
                                    "local_temp": "/usr/local/airflow/temp_web",
                                    "key_name": "hung/web_events"})

    load_files_s3_app = PythonOperator(task_id='load_files_to_s3_app', provide_context=True, python_callable=upload_files_to_s3,
                                op_kwargs = {
                                    "bucket_name": "luko-data-eng-exercice",
                                    "local_temp": "/usr/local/airflow/temp_app",
                                    "key_name": "hung/app_events"})

    create_table_web_events = PostgresOperator(task_id="create_table_web_events",
                                    postgres_conn_id="redshift",
                                    sql=create_airtable_web_events)

    create_table_app_events = PostgresOperator(task_id="create_table_app_events",
                                    postgres_conn_id="redshift",
                                    sql=create_airtable_app_events)

    write_files_redshift_web = PythonOperator(task_id='write_files_redshift_web', provide_context=True, python_callable=write_s3_to_warehouse,
                                op_kwargs = {
                                    "sql_statement": copy_all_web_sql,
                                    })

    write_files_redshift_app = PythonOperator(task_id='write_files_redshift_app', provide_context=True, python_callable=write_s3_to_warehouse,
                                op_kwargs = {
                                    "sql_statement": copy_all_app_sql,
                                    })

    sql_raw_event = PostgresOperator(task_id="sql_raw_event",
                                    postgres_conn_id="redshift",
                                    sql=raw_event_table_value)

    sql_event_sequence = PostgresOperator(task_id="sql_event_sequence",
                                    postgres_conn_id="redshift",
                                    sql=event_sequence)

    sql_event_metrics = PostgresOperator(task_id="sql_event_metrics",
                                    postgres_conn_id="redshift",
                                    sql=event_metrics)

    sql_attribution = PostgresOperator(task_id="sql_attribution",
                                    postgres_conn_id="redshift",
                                    sql=attribution_table)

    clean_up_files = PythonOperator(task_id='clean_up_files', python_callable=clean_up)

    api_get_web >> load_files_s3_web >> write_files_redshift_web
    api_get_app >> load_files_s3_app >> write_files_redshift_app

    create_table_web_events >> write_files_redshift_web
    create_table_app_events >> write_files_redshift_app

    write_files_redshift_web >> sql_raw_event
    write_files_redshift_app >> sql_raw_event

    sql_raw_event >> sql_event_sequence >> sql_event_metrics >> sql_attribution >> clean_up_files



    
   