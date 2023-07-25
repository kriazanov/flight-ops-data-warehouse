"""
**Description**: XML Datafile from Merdian for Flight Datamart from board.xml
**Target**: `raw_meridian_board_xml`
**Call**: `sf_transform_meridian_xml_data_ops`
"""

import pendulum
import smbclient
import logging
from pendulum import datetime as dttm

from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=4),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 */6 * * *',
    'description': 'XML Datafile from Merdian for Flight Datamart from board.xml',
    'tags': ['dwh', 'ops', 'meridian', 'xml'],
    'max_active_runs': 1
}

SMB_USERNAME = 'username'
SMB_PASSWORD = 'password'
SMB_HOSTNAME = '10.96.0.1'


@dag(**DAG_PARAMS)
def dwh_meridian_sap_xml_ops():
    @task()
    def extract(**kwargs):
        try:
            ti: TaskInstance = kwargs['task_instance']
            fdt = dttm(*[int(dpart) for dpart in '2022-03-21'.split('-')])

            smbclient.ClientConfig(username=SMB_USERNAME, password=SMB_PASSWORD)
            smbclient.register_session(SMB_HOSTNAME, username=SMB_USERNAME, password=SMB_PASSWORD)
            file_name = f'//{SMB_HOSTNAME}/Meridian_exports/board.xml'
            with smbclient.open_file(file_name, mode='r', encoding='utf-8') as f:
                file_content = f.read()
            query = f'''
                insert into dwh_staging.raw_meridian_board_xml(file_content, date_created, date_processed)
                values('{file_content}', now(), null)
            '''
            return query
        except OSError as s:
            logging.error(s)

    @task()
    def load(query):
        target_hook = PostgresHook('conn_dwh')
        target_hook.run(query)

    @task()
    def transform():
        target_hook = PostgresHook('conn_dwh')
        target_hook.run(f'''select dwh_staging.sf_transform_meridian_xml_data_ops()''')

    # Operators
    t_extract = extract()
    t_load = load(t_extract)
    t_transform = transform()

    # Pipeline
    t_extract >> t_load >> t_transform


root = dwh_meridian_sap_xml_ops()
