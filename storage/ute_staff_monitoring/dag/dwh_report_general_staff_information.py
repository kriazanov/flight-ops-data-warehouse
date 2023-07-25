"""
**Description**: General Staff Information
**Target**: `report_general_staff_information`
**Call**: `sf_prepare_report_general_staff_information`
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
    'description': 'General Staff Information',
    'tags': ['dwh', 'amos', 'helicopters', 'sql'],
    'max_active_runs': 1
}


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_report_general_staff_information():
    @task()
    def extract():
        query = 'SELECT * FROM info_amos.dwh_heli_006()'
        logging.info(f'Running SQL Script:\n{query}')
        dataset = get_hook_source_amos_heli().get_records(query)

        get_hook_target_dwh().run('truncate table dwh_staging.report_general_staff_information;')

        query = """\n
        insert into dwh_staging.report_general_staff_information(
            login, first_last_name, birthday, sex, otdel, class, rating_date, work_place_number, work_place,
            zup_empl_guid, personnel_number, zfo, job_title, specialization, start_work_date, end_work_date,
            work_status, work_place2, location, subdivision, work_shift, certificate_number, total_work_experience,
            apk_validity_date, certificate_give_date, validity_authorisation_date, status)
        values"""

        query = query + """(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s),\n""" * len(dataset)

        query = query.rstrip(',\n')
        query = query + ';'
        dataset = [item for sublist in dataset for item in sublist]
        get_hook_target_dwh().run(query, parameters=dataset)

    @task()
    def prepare():
        get_hook_target_dwh().run('select dwh_staging.sf_prepare_report_general_staff_information()')

    # Operators
    t_extract = extract()
    t_prepare = prepare()

    # Pipeline
    t_extract >> t_prepare


root = dwh_report_general_staff_information()
root.doc_md = __doc__
