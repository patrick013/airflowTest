from airflow import DAG
import os
import json
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

DAG_NAME = "demo_0829"

# Configure
SFTP_SRC_PATH = '/home/vmadmin/airflow/temp_files/'
TODAY_TEST_TIME = str(datetime.now())
AZURE_CONTAINER_NAME = "myairflow"

# CONNECTION IDS
AZURE_BLOB_CONN_ID = "a_demo_testairflowstorage_id"
SFTP_CONN_ID = "a_demo_sftp_id"
HTTP_CONN_ID = "a_demo_http_id"
AZURE_SQLDB_CONN_ID = "a_demo_mssql_id"


def flatten_json(input_json):
    """
    To flatten Product Info Json format
    """
    result = []
    for itm in input_json:
        flatten_itm = {}
        for i_key, i_value in itm.items():
            if type(i_value) is dict:
                for i_value_k, i_value_v in i_value.items():
                    flatten_itm[i_value_k] = i_value_v
            else:
                flatten_itm[i_key] = i_value
        result.append(flatten_itm)
    return result


def join_data(task_instance):
    product_data = flatten_json(task_instance.xcom_pull(task_ids='extract_product_info'))
    exposure_data = task_instance.xcom_pull(task_ids='extract_exposure_info')
    product_df = pd.DataFrame(product_data)
    exposure_df = pd.DataFrame(exposure_data)
    joined_df = product_df.join(exposure_df.set_index('sku_id'), on='sku_id')

    return joined_df


def agg_data(task_instance):
    joined_df = task_instance.xcom_pull(task_ids='join_extracted_data')
    transformed_df = joined_df.groupby(['sku_id', 'discount']).agg({'impressions': 'sum',
                                                                    'clicks': 'sum',
                                                                    'buys': 'sum',
                                                                    'datetime': 'min'})
    transformed_df['CTR'] = round(transformed_df['clicks'] / transformed_df['impressions'], 3)
    transformed_df['CVR'] = round(transformed_df['buys'] / transformed_df['impressions'], 3)
    transformed_df.reset_index(inplace=True)

    filepath = Path(SFTP_SRC_PATH + TODAY_TEST_TIME + '.csv')
    transformed_df.to_csv(filepath, index=False)


default_args = {
    'owner': 'patrickruan',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    #  'retries':1,
    #  'retry_dalay':timedelta(minutes=2)
}

with DAG(DAG_NAME,
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    check_api_server = HttpSensor(
        task_id='check_api_server',
        http_conn_id='a_demo_http_id',
        endpoint='/patrick013/myFakedataAPI'
    )

    extract_product_info = SimpleHttpOperator(
        task_id='extract_product_info',
        http_conn_id=HTTP_CONN_ID,
        endpoint='/patrick013/myFakedataAPI/Products',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    extract_exposure_info = SimpleHttpOperator(
        task_id='extract_exposure_info',
        http_conn_id=HTTP_CONN_ID,
        endpoint='/patrick013/myFakedataAPI/Exposure',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    join_extracted_data = PythonOperator(
        task_id='join_extracted_data',
        python_callable=join_data
    )


    @dag.task(task_id="insert_exposure_sqldb")
    def insert_exposure_sqldb(ti=None):
        joined_df = ti.xcom_pull(task_ids='join_extracted_data')
        result_df = joined_df[
            ['product_name',
             'unit_price',
             'discount',
             'category_code',
             'sku_id',
             'category',
             'color_code',
             'size',
             'suitable',
             'impressions',
             'clicks',
             'buys',
             'start_timestamp',
             'end_timestamp',
             'datetime']].copy()
        mssql_hook = MsSqlHook(mssql_conn_id=AZURE_SQLDB_CONN_ID, schema="ADFGeneralDB")
        rows = [tuple(r) for r in result_df.to_numpy()]
        target_fields = list(result_df)
        mssql_hook.insert_rows(table="airflow.ExposureTable", rows=rows, target_fields=target_fields)


    agg_extracted_data = PythonOperator(
        task_id='agg_extracted_data',
        python_callable=agg_data
    )

    upload_file_to_azure = SFTPToWasbOperator(
        task_id="upload_file_to_azure",
        sftp_conn_id=SFTP_CONN_ID,
        wasb_conn_id=AZURE_BLOB_CONN_ID,
        sftp_source_path=SFTP_SRC_PATH + '*',
        container_name=AZURE_CONTAINER_NAME,
        blob_prefix='{{run_id}}' + "_",
        wasb_overwrite_object=True
    )

    clear_temp_files = BashOperator(
        task_id="clear_temp_files",
        bash_command='rm -r ' + SFTP_SRC_PATH + '*',
        trigger_rule='all_done'
    )

    check_api_server >> [extract_product_info, extract_exposure_info] >> join_extracted_data
    join_extracted_data >> insert_exposure_sqldb() >> clear_temp_files
    join_extracted_data >> agg_extracted_data >> upload_file_to_azure >> clear_temp_files
