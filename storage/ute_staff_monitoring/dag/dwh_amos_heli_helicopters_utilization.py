"""
**Description**: Helicopters Utilization
**Target**: `report_helicopters_utilization`
**Call**: `sf_perform_report_helicopters_utilization`
"""

import logging
import os
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=2),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '15 4 8 * *',
    'description': 'Helicopters Utilization',
    'tags': ['dwh', 'amos', 'helicopters', 'sql', 'report'],
    'max_active_runs': 1
}

QUERY_PATH = os.path.join(os.path.dirname(__file__), 'queries/helicopters_utilization.sql')


def get_hook_target_dwh():
    return PostgresHook('conn_dwh')


def get_hook_source_amos_heli():
    return PostgresHook('conn_amos_helicopters')


@dag(**DAG_PARAMS)
def dwh_report_helicopters_utilization():
    @task()
    def extract(**kwargs):
        ti: TaskInstance = kwargs['task_instance']
        report_time = ti.execution_date - timedelta(days=30)

        period_month = report_time.strftime("%m")
        period_year = report_time.strftime("%Y")

        src_query = f'select * from info_amos.dwh_heli_009({str(int(period_month))}, {str(int(period_year))});'
        dataset = get_hook_source_amos_heli().get_records(src_query)

        # for record in dataset:
        tgt_query = """\nINSERT INTO dwh_staging.report_helicopters_utilization values"""
        add_params = """(%s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
        %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
        %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar, %s::varchar,
        %s::varchar, %s::varchar, %s::varchar, %s::varchar, false),\n""" * len(dataset)
        tgt_query += add_params

        tgt_query = tgt_query.rstrip(',\n')
        tgt_query = tgt_query + ';'
        dataset = [item if item != '' else None for sublist in dataset for item in sublist]

        get_hook_target_dwh().run(tgt_query, parameters=dataset)

    @task()
    def perform():
        get_hook_target_dwh().run('select dwh_staging.sf_perform_report_helicopters_utilization()')

    # Operators
    t_extract = extract()
    t_perform = perform()

    # Pipeline
    t_extract >> t_perform


root = dwh_report_helicopters_utilization()
root.doc_md = __doc__
