"""
**Description**: Helicopters Station
**Target**: `report_helicopters_station`
**Call**: `sf_prepare_report_helicopters_station`
"""

import logging
import os

from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=5),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '20 4 8 * *',
    'description': 'Helicopters Station',
    'tags': ['dwh', 'amos', 'helicopters', 'sql'],
    'max_active_runs': 1
}


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_report_helicopters_station():
    @task()
    def extract():
        query = 'select * from info_amos.dwh_heli_007();'
        logging.info(f'Running SQL Script:\n{query}')
        dataset = get_hook_source_amos_heli().get_records(query)

        get_hook_target_dwh().run('truncate table dwh_staging.report_helicopters_station;')

        query = """\n
        insert into dwh_staging.report_helicopters_station(
            station, vendor, name, name_1, name_2, zip_code, city, state, country, department_utvu, department_uti,
            longitude, latitude)
        values"""

        query = query + """(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),\n""" * len(dataset)

        query = query.rstrip(',\n')
        query = query + ';'
        dataset = [item for sublist in dataset for item in sublist]
        get_hook_target_dwh().run(query, parameters=dataset)

    @task()
    def prepare():
        get_hook_target_dwh().run('select dwh_staging.sf_prepare_report_helicopters_station()')

    # Operators
    t_extract = extract()
    t_prepare = prepare()

    # Pipeline
    t_extract >> t_prepare


root = dwh_report_helicopters_station()
root.doc_md = __doc__
