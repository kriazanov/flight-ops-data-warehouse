"""
**Description**: XML Datafile from Merdian for Flight Datamart from XML files, generated for SAP
**Target**: `raw_meridian_sap_xml`
**Call**: `sf_transform_meridian_xml_data_sal`
"""

import smbclient
import logging

from pendulum import datetime as dttm

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=5),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2019, 8, 6, 00, 00, 0, 0),
    'schedule_interval': '0 12 * * *',
    'description': 'XML Datafile from Merdian for Flight Datamart from XML files, generated for SAP',
    'tags': ['dwh', 'sal', 'meridian', 'xml'],
    'max_active_runs': 1
}

SMB_USERNAME = 'username'
SMB_PASSWORD = 'password'
SMB_HOSTNAME = '10.96.0.1'

@dag(**DAG_PARAMS)
def dwh_meridian_sap_xml_sal():
    @task()
    def extract(**kwargs):
        try:
            ti: TaskInstance = kwargs['task_instance']
            fdt = dttm(*[int(dpart) for dpart in '2022-03-21'.split('-')])

            smbclient.ClientConfig(username=SMB_USERNAME, password=SMB_PASSWORD)
            smbclient.register_session(SMB_HOSTNAME, username=SMB_USERNAME, password=SMB_PASSWORD)
            filename_datepart = ti.execution_date  # - timedelta(days=1)
            filename = f'exp_{filename_datepart.strftime("%Y%m%d")}_sap.xml'
            file_name = f'//{smb_hostname}/Meridian_exports/asia_exp/SAP/.done/{filename}'
            with smbclient.open_file(file_name, mode='r', encoding='utf-8') as f:
                file_content = f.read()
            query = f'''
                insert into dwh_staging.raw_meridian_sap_xml(file_name, file_content, date_created, date_processed)
                values('{filename}', '{file_content}', now(), null)
                on conflict(file_name) do nothing
            '''

            return query
        except OSError as s:
            logging.error(s)

    @task()
    def load(query):
        target_hook = PostgresHook('conn_dwh')
        logging.info(query)
        target_hook.run(query)

    @task()
    def transform():
        target_hook = PostgresHook('conn_dwh')
        target_hook.run(f'''select dwh_staging.sf_transform_meridian_xml_data_sal()''')

    # Operators
    t_extract = extract()
    t_load = load(t_extract)
    t_transform = transform()

    # Pipeline
    t_extract >> t_load >> t_transform


root = dwh_meridian_sap_xml_sal()
