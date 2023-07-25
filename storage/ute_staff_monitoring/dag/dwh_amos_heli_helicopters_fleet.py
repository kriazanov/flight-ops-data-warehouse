"""
**Description**: Helicopters Fleet
**Target**: `report_general_staff_information`
**Call**: `report_helicopters_fleet`
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
    'schedule_interval': '10 4 8 * *',
    'description': 'Helicopters Fleet',
    'tags': ['dwh', 'amos', 'helicopters', 'sql'],
    'max_active_runs': 1
}

QUERY_PATH = os.path.join(os.path.dirname(__file__), 'queries/helicopters_fleet.sql')


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_report_helicopters_fleet():
    @task()
    def extract():
        query = 'select * from info_amos.dwh_heli_008();'
        dataset = get_hook_source_amos_heli().get_records(query)

        get_hook_target_dwh().run('truncate table dwh_staging.report_helicopters_fleet;')

        query = """\ninsert into dwh_staging.report_helicopters_fleet values"""
        query += """(%s, %s, %s, %s, %s, %s, %s, to_date(%s, 'DD.MM.YYYY'), to_date(%s, 'DD.MM.YYYY'), 
                    to_date(%s, 'DD.MM.YYYY'), to_date(%s, 'DD.MM.YYYY'), to_date(%s, 'DD.MM.YYYY'), %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),\n""" * len(dataset)
        query = query.rstrip(',\n')
        query = query + ';'

        dataset = [item for sublist in dataset for item in sublist]
        get_hook_target_dwh().run(query, parameters=dataset)

    @task()
    def prepare():
        get_hook_target_dwh().run('select dwh_staging.sf_prepare_report_helicopters_fleet()')

    # Operators
    t_extract = extract()
    t_prepare = prepare()

    # Pipeline
    t_extract >> t_prepare


root = dwh_report_helicopters_fleet()
root.doc_md = __doc__
