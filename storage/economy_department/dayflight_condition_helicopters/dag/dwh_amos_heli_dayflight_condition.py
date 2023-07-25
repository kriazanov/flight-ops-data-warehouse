"""
**Description**: Aircraft Availability Report
**Target**: `report_dayflight_condition_helicopters`
**Call**: `sf_prepare_heli_dayflight_condition`
"""

import logging
import os
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=10),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 4 * * *',
    'description': 'Aircraft Availability Report',
    'tags': ['dwh', 'amos', 'helicopters', 'sql'],
    'max_active_runs': 1
}

DATA_PARTS = 10


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_amos_heli_dayflight_condition():
    @task()
    def extract():
        query = '''
            SELECT *
            FROM info_amos.dwh_heli_001(('01.01.' || extract(year from now()))::date - '1971-12-31'::date,current_date - '1971-12-31'::date);
        '''

        dataset = get_hook_source_amos_heli().get_records(query)
        list_size = len(dataset) // DATA_PARTS  # 837925

        for i in range(DATA_PARTS):
            query = '\ninsert into dwh_staging.report_dayflight_condition_helicopters(ac_type, ac_registr, dislocation, base, base_date, marker, color, flight_minutes) values \n'
            new_short_data_set = dataset[i * list_size: (i + 1) * list_size]

            for record in new_short_data_set:
                query += f"('{record[0]}', '{record[1]}', nullif('{record[2]}', 'None'), nullif('{record[3]}', 'None'), '{record[4]}', nullif('{record[5]}', 'None'), nullif('{record[6]}', 'None'), nullif('{record[7]}', 'Исправен-не летал')::numeric),\n"

            query = query.rstrip(',\n')
            get_hook_target_dwh().run(query)

    @task
    def prepare():
        get_hook_target_dwh().run('select dwh_staging.sf_prepare_heli_dayflight_condition()')

    # Operators
    t_extract = extract()
    t_prepare = prepare()

    # Pipeline
    t_extract >> t_prepare


root = dwh_amos_heli_dayflight_condition()
root.doc_md = __doc__
