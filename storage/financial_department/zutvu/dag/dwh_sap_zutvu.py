"""
**Description**: Retrieving information about ZUTVU financial transactions
**Target**: `raw_sap_zutvu_soap_response`
**Call**: `sf_prepare_sap_zutvu`
"""

import logging
import requests
import pendulum

from datetime import datetime, date, timedelta
from airflow.decorators import dag, task
from airflow.providers.imap.hooks.imap import ImapHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.auth import HTTPBasicAuth

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=10),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 16, 0, 0, 0, 0),
    'schedule_interval': '0 11 * * *',
    'description': 'Retrieving information about ZUTVU financial transactions',
    'tags': ['dwh', 'sap', 'soap'],
    'max_active_runs': 1
}

TGT_HOOK_CONN_NAME = 'conn_dwh'
SRC_HOOK_CONN_NAME = 'conn_imap_sap'
URL = "http://hostname.com/url/to/zutvu"
HEADERS = {'content-type': 'text/xml'}


def create_body():
    last_day_report = datetime.today()
    first_day_report = date(last_day_report.year, 1, 1)
    logging.info(f'last_day_report = {last_day_report}; \nfirst_day_report = {first_day_report}')
    body = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:sap-com:document:sap:rfc:functions">
               <soapenv:Header/>
               <soapenv:Body>
                  <urn:Z_MBI_ZUT_BILL>
                     <IV_BFKDAT>{first_day_report.strftime('%Y-%m-%d')}</IV_BFKDAT>
                     <IV_EFKDAT>{last_day_report.strftime('%Y-%m-%d')}</IV_EFKDAT>
                     <IV_VKORG>UT12</IV_VKORG>
                  </urn:Z_MBI_ZUT_BILL>
               </soapenv:Body>
            </soapenv:Envelope>
            """
    return body


def get_source_hook_imap():
    return ImapHook(SRC_HOOK_CONN_NAME)


def get_target_hook_dwh():
    return PostgresHook(TGT_HOOK_CONN_NAME)


def get_username_imap():
    return get_source_hook_imap().get_connection(SRC_HOOK_CONN_NAME).login


def get_password_imap():
    return get_source_hook_imap().get_connection(SRC_HOOK_CONN_NAME).password


@dag(**DAG_PARAMS)
def dwh_sap_zut_bill():
    @task()
    def extract(**kwargs):
        body = create_body()
        return body

    @task()
    def load(body):
        response = requests.post(URL, data=body, headers=HEADERS, auth=HTTPBasicAuth(get_username_imap(), get_password_imap()))
        truncate_query = 'truncate table dwh_staging.raw_sap_zut_bill_soap_response'
        insert_query = f'''
            insert into dwh_staging.raw_sap_zut_bill_soap_response(response_content, date_created, date_processed)
            values ('{response.text}', now(), null)'''
        get_target_hook_dwh().run(truncate_query)
        get_target_hook_dwh().run(insert_query)

    @task()
    def transform():
        get_target_hook_dwh().run("select dwh_staging.sf_transform_sap_zut_bill()")

    # Operators
    t_extract = extract()
    t_load = load(t_extract)
    t_transform = transform()

    # Pipeline
    t_extract >> t_load >> t_transform


root = dwh_sap_zut_bill()
root.doc_md = __doc__
