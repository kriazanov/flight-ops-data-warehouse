"""
**Description**: Salary Piecework
**Target**: `report_salary_piecework`
**Call**: `sf_perform_report_salary_piecework`
"""

import logging
import os
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.relativedelta import relativedelta

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=10),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2022, 6, 1, 00, 00, 0, 0),
    'schedule_interval': '0 5 8 * *',
    'description': 'Salary Piecework',
    'tags': ['dwh', 'amos', 'helicopters', 'sql', 'report'],
    'max_active_runs': 1
}

DATA_PARTS = 10


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_report_salary_piecework():
    @task()
    def extract(**kwargs):
        query_truncate = "truncate dwh_staging.report_salary_piecework;"
        get_hook_target_dwh().run(query_truncate)

        ti: TaskInstance = kwargs['task_instance']
        report_time_start = ti.execution_date
        report_time_end = report_time_start - relativedelta(years=1)
        src_query = f"select * from info_amos.dwh_heli_003('{report_time_end.strftime('%m.%d.%Y')}', '{report_time_start.strftime('%m.%d.%Y')}');"

        logging.info(src_query)
        dataset = get_hook_source_amos_heli().get_records(src_query)

        list_size = len(dataset) // DATA_PARTS  # 837925

        for i in range(DATA_PARTS):
            tgt_query = """\n
                insert into dwh_staging.report_salary_piecework(
                    task_reference, descno_i, ext_workorderno, event_perfno_i, created_date, issue_date, issue_time,
                    closing_date, closing_time, description, special, special_vne, prj_description, issue_station2,
                    ac_registr, ac_type, owner_name, ws_count, total, b13, b2, lab_d, lab_aiero, tr, pp, ogm, fact_tr,
                    at_code, task_ref_diff, st_d, end_d, meta_key, period, issue_sign, closing_sign, issue_sign_short,
                    mech_sign, text_task, rem_text, wpno, rlmt, tip_to, state)
                values"""

            new_short_data_set = dataset[i * list_size: (i + 1) * list_size]

            add_params = """(%s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
                            %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
                            %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
                            %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
                            %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
                            %s::varchar, %s::varchar, %s::varchar),\n""" * len(new_short_data_set)
            tgt_query += add_params

            tgt_query = tgt_query.rstrip(',\n')
            new_short_data_set = [item if item != '' else None for sublist in new_short_data_set for item in sublist]
            get_hook_target_dwh().run(tgt_query, parameters=new_short_data_set)

    @task()
    def perform():
        get_hook_target_dwh().run('select dwh_staging.sf_perform_report_salary_piecework()')

    # Operators
    t_extract = extract()
    t_perform = perform()

    # Pipeline
    t_extract >> t_perform


root = dwh_report_salary_piecework()
root.doc_md = __doc__
