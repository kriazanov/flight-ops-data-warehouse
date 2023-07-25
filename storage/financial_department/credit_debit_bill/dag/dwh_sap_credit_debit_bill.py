"""
**Description**: Credit and Debit debts information ETL delivery
**Target**: `raw_sap_credit_debit_bill_soap_response`
**Call**: `sf_prepare_sap_debit_credit_bill`
"""

import logging
import pendulum
import requests

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
    'description': 'Credit and Debit debts information ETL delivery',
    'tags': ['dwh', 'sap', 'soap'],
    'max_active_runs': 1
}

TGT_HOOK_CONN_NAME = 'conn_dwh'
SRC_HOOK_CONN_NAME = 'conn_imap_sap'
URL = "http://hostname.com/path/to/report"
HEADERS = {'content-type': 'text/xml'}


def create_body(debit_credit):
    last_day_report = datetime.today()
    logging.info(f'last_day_report = {last_day_report};')
    body = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:sap-com:document:sap:rfc:functions">
               <soapenv:Header/>
               <soapenv:Body>
                  <urn:Z_MBI_ZFI_OP_REP>
                    <IV_BUKRS>UT12</IV_BUKRS>
                    <IV_EFKDAT>{last_day_report.strftime('%Y-%m-%d')}</IV_EFKDAT>
                    <IV_KOART>{debit_credit}</IV_KOART>
                  </urn:Z_MBI_ZFI_OP_REP>
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
def dwh_sap_credit_debit_bill():
    @task()
    def extract():
        truncate_query = f'truncate table dwh_staging.raw_sap_credit_debit_bill_soap_response;'
        get_target_hook_dwh().run(truncate_query)
        for i in ['D', 'K']:
            body = create_body(i)
            response = requests.post(URL, data=body, headers=HEADERS,
                                     auth=HTTPBasicAuth(get_username_imap(), get_password_imap()))
            insert_query = f'''
                        insert into dwh_staging.raw_sap_credit_debit_bill_soap_response(response_content, date_created, date_processed, debit_or_credit)
                        values (%s, now(), null, '{i}');'''
            get_target_hook_dwh().run(insert_query, parameters=(str(response.text),))

    @task()
    def prepare():
        get_target_hook_dwh().run("select dwh_staging.sf_prepare_sap_debit_credit_bill();")

    # Operators
    t_extract = extract()
    t_prepare = prepare()

    # Pipeline
    t_extract >> t_prepare


root = dwh_sap_credit_debit_bill()
root.doc_md = __doc__
