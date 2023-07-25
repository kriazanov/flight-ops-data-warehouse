"""
**Description**: Decoding flight data from flight recorder
**Target**: `fact_flight_millisecond`
**Call**: `sf_transform_staffonly_flight_data`
"""

import gzip
import os
import logging
import shutil
import time
import paramiko

from datetime import timedelta, datetime

from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.decorators import dag, task
from io import BytesIO

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=30),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2022, 5, 2, 00, 00, 0, 0),
    'schedule_interval': '0 0 * * *',
    'description': 'Загрузка данных бортовых самописцев в КХД',
    'tags': ['dwh', 'staffonly', 'tsc'],
    'max_active_runs': 1
}

LOCAL_STORAGE_PATH = '/tmp/dwh_staffonly_aircraft_files/'
TEMP_FTP_UTILITY_BASE_PATH_APP = 'E:\\ftp\\winarm_utility\\decodetsc.exe'
TEMP_FTP_UTILITY_BASE_PATH_HDR = 'E:\\ftp\\winarm_files\\hdr'
TEMP_FTP_UTILITY_BASE_PATH_TSC = 'E:\\ftp\\winarm_files\\tsc'

FTP_ENCODING = 'Windows-1251'
MAX_SOURCE_DOWNLOAD_ATTEMPTS = 3
SOURCE_DOWNLOAD_BLOCK_SIZE = 8 * 1024
TSC_MIDFILE_PAUSE_TIME_SECONDS = 5

FILES_TO_UPLOAD = ['9.txt', '10.txt', '14.txt', '15.txt', '16.txt', '17.txt', '20.txt', '21.txt', '22.txt', '66.txt',
                   '67.txt', '68.txt', '69.txt', '1301.txt', '1302.txt', '1303.txt', '1304.txt', '1314.txt', '1315.txt',
                   '1318.txt', '1319.txt']


def get_dwh_hook():
    return PostgresHook('conn_dwh')


def get_ssh_hook():
    return SSHHook('ssh_winarm_ftp')


def get_src_ftp_hook():
    return FTPHook('ftp_staffonly')


def get_tmp_ftp_hook():
    return FTPHook('ftp_winarm_temp')


def get_src_ftp_list_folders(conn_src_ftp):
    result = []
    ftp_list = []
    conn_src_ftp.retrlines('LIST', ftp_list.append)
    for line in ftp_list:
        line = line.split(None, 8)
        foldername = line[8]
        if foldername not in ('.', '..'):
            result.append(foldername)
            result.append(foldername + '/Сделано')
    return result


def get_src_ftp_list_files(conn_tmp_ftp, date_filter):
    result = []
    ftp_list = []
    conn_tmp_ftp.retrlines('LIST', ftp_list.append)
    for line in ftp_list:
        line = line.split(None, 8)
        filename, file_extention = os.path.splitext(line[8])
        if filename not in ('.', '..') and file_extention in ('.tsc', '.gz') and filename.split('_')[1] == date_filter:
            result.append(filename + file_extention)
    return result


def get_tmp_ftp_list_folders(conn_tmp_ftp):
    result = []
    ftp_list = []
    conn_tmp_ftp.retrlines('LIST', ftp_list.append)
    for line in ftp_list:
        line = line.split(None, 8)
        foldername = line[8]
        if line[4] == '0':
            result.append(foldername)
    return result


def get_tmp_ftp_list_files(conn_tmp_ftp):
    result = []
    ftp_list = []
    conn_tmp_ftp.retrlines('LIST', ftp_list.append)
    for line in ftp_list:
        line = line.split(None, 8)
        filename, file_extention = os.path.splitext(line[8])
        foldername = line[8]
        if file_extention == '.txt':
            result.append(foldername)
    return result


def create_temp_dir():
    shutil.rmtree(LOCAL_STORAGE_PATH, ignore_errors=True)
    try:
        os.mkdir(LOCAL_STORAGE_PATH)
        logging.info('Local storage created')
    except FileExistsError:
        logging.info('Local storage already exists')


def get_ftp_aircraft_filenames(conn_src_ftp, date_filter):
    result = []
    ftp_folders = get_src_ftp_list_folders(conn_src_ftp)
    for ftp_folder in ftp_folders:
        conn_src_ftp.cwd(ftp_folder)
        ftp_files = get_src_ftp_list_files(conn_src_ftp, date_filter)
        for ftp_file in ftp_files:
            result.append(f"{ftp_folder}/{ftp_file}")
        conn_src_ftp.cwd('/')

    return result


def download_files_from_source_ftp(conn_src_ftp, date):
    attempt = 1
    while attempt <= MAX_SOURCE_DOWNLOAD_ATTEMPTS:
        logging.info(f"Download attempt: {attempt}")
        try:
            filepaths = get_ftp_aircraft_filenames(conn_src_ftp, date)

            for filepath in filepaths:
                dirname = os.path.dirname(filepath)
                filename = os.path.basename(filepath)
                conn_src_ftp.cwd(dirname)
                local_filename = os.path.join(LOCAL_STORAGE_PATH, filename)
                lf = open(local_filename, "wb")
                conn_src_ftp.retrbinary("RETR " + filename, lf.write, SOURCE_DOWNLOAD_BLOCK_SIZE)
                lf.close()
                conn_src_ftp.cwd('/')
            break
        except ConnectionRefusedError:
            if attempt == MAX_SOURCE_DOWNLOAD_ATTEMPTS:
                raise ConnectionRefusedError
            attempt += 1
            logging.info(f"Source FTP connection refused")

    logging.info(f"Download done with attempt {attempt}")


def unzip_local_stored_files(directory):
    extension = ".gz"
    os.chdir(directory)
    for file in os.listdir(directory):
        if file.endswith(extension):
            gz_name = os.path.abspath(file)
            file_name = (os.path.basename(gz_name)).rsplit('.', 1)[0]
            with gzip.open(gz_name, "rb") as f_in, open(file_name, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(gz_name)


def upload_local_stored_files_to_temp_ftp(conn_tmp_ftp):
    conn_tmp_ftp.cwd('/')
    conn_tmp_ftp.cwd("/ftp/winarm_files/tsc/")
    for filename in os.listdir(LOCAL_STORAGE_PATH):
        with open(os.path.join(LOCAL_STORAGE_PATH, filename), 'rb') as file:
            conn_tmp_ftp.storbinary(f"STOR {filename}", file)


def call_remote_ftp_utility(conn_ssh, filename, path_app, path_hdr, path_tsc):
    ssh_stdin, ssh_stdout, ssh_stderr = '', '', ''
    cmd = f"{path_app} {path_hdr} {path_tsc}"
    try:
        ssh_stdin, ssh_stdout, ssh_stderr = conn_ssh.exec_command(cmd)
        logging.info(f"{filename}: Command '{cmd}' executed")
    except Exception:
        logging.info(
            f"{filename}: Command '{cmd}' executed with error '{ssh_stderr}', StdIn:'{ssh_stdin}' StdOut:'{ssh_stdout}'")


def decode_files_on_temp_ftp(conn_tmp_ftp, conn_ssh):
    ftp_list = []
    conn_tmp_ftp.cwd('/')
    conn_tmp_ftp.cwd('ftp/winarm_files/tsc')
    conn_tmp_ftp.retrlines('LIST', ftp_list.append)

    for line in ftp_list:
        filename = line.split(None, 8)[8]
        tail_no = filename.split('_')[0]

        path_app = TEMP_FTP_UTILITY_BASE_PATH_APP
        path_hdr = f'{TEMP_FTP_UTILITY_BASE_PATH_HDR}\\{tail_no}\\{tail_no}.HDR'
        path_tsc = f'{TEMP_FTP_UTILITY_BASE_PATH_TSC}\\{filename}'

        call_remote_ftp_utility(conn_ssh, filename, path_app, path_hdr, path_tsc)
        time.sleep(TSC_MIDFILE_PAUSE_TIME_SECONDS)

    conn_ssh.close()


def build_insert_query(dataset):
    query = 'INSERT INTO dwh_staging.raw_staffonly_files_decrypted(tail_no, end_record, param_no, param_key, param_value) VALUES\n'
    if dataset:
        for line in dataset:
            dataset_tail_no, dataset_end_record_date, dataset_end_record_time, dataset_param_no, dataset_param_key, dataset_param_value = line
            query += f"('{dataset_tail_no}', to_timestamp('{dataset_end_record_date} {dataset_end_record_time}', 'YYYYMMDD HH24MISS'), {dataset_param_no}, '{dataset_end_record_date} {dataset_param_key}', {dataset_param_value.strip()}::float),\n"
        query = query.rstrip(',\n') + ';'
    else:
        query = 'select null;'
    return query


def load_decrypted_data(conn_tmp_ftp):
    get_dwh_hook().run('truncate table dwh_staging.raw_staffonly_files_decrypted;')
    conn_tmp_ftp.cwd('/')
    conn_tmp_ftp.cwd('/ftp/winarm_files/tsc/')
    decrypted_folders = get_tmp_ftp_list_folders(conn_tmp_ftp)
    work_times = []
    i = len((decrypted_folders))

    for decrypted_folder in decrypted_folders:
        start = datetime.now()
        i = i - 1
        logging.info(f"Processing folder {decrypted_folder}")
        dataset_tail_no, dataset_end_record_date, dataset_end_record_time = decrypted_folder.split('_')
        conn_tmp_ftp.cwd(decrypted_folder)
        decrypted_files = get_tmp_ftp_list_files(conn_tmp_ftp)

        for decrypted_file in decrypted_files:
            if decrypted_file in FILES_TO_UPLOAD:
                dataset_data = []
                decrypted_content = BytesIO()
                dataset_param_no = decrypted_file.split('.')[0]
                conn_tmp_ftp.retrbinary(f'RETR {decrypted_file}', decrypted_content.write)
                decrypted_data = decrypted_content.getvalue().decode(encoding='Windows-1251')
                decrypted_lines = decrypted_data.split('\n')

                for decrypted_line in decrypted_lines:
                    try:
                        dataset_param_key, dataset_param_value = decrypted_line.split('\t')

                        dataset_line = [dataset_tail_no, dataset_end_record_date, dataset_end_record_time,
                                        dataset_param_no, dataset_param_key, dataset_param_value]
                        dataset_data.append(dataset_line)
                    except ValueError:
                        pass

                query = build_insert_query(dataset_data)

                try:
                    get_dwh_hook().run(query)
                except Exception:
                    logging.info(f"Syntax error in query: '{query}'")
                    continue

                decrypted_content.close()
                conn_tmp_ftp.delete(decrypted_file)
            else:
                conn_tmp_ftp.delete(decrypted_file)

        conn_tmp_ftp.delete(f'{decrypted_folder}.arm')
        conn_tmp_ftp.cwd('..')
        conn_tmp_ftp.rmd(decrypted_folder)
        conn_tmp_ftp.delete(f'{decrypted_folder}.tsc')

        end = datetime.now()
        work_times.append(end - start)
        logging.info(f"Pass time: {sum(work_times, timedelta(0,0,0))}")
        logging.info(f'Got about {(sum(work_times, timedelta(0,0,0)) / len(work_times)) * i}')
    get_dwh_hook().run('select dwh_staging.sf_transform_staffonly_flight_data();')


def cleanup_non_loaded_folders(conn_tmp_ftp):
    conn_tmp_ftp.cwd('/')
    conn_tmp_ftp.cwd("/ftp/winarm_files/tsc/")
    for folder in get_tmp_ftp_list_folders(conn_tmp_ftp):
        conn_tmp_ftp.cwd(folder)
        for file in get_tmp_ftp_list_files(conn_tmp_ftp):
            logging.info(f'DELETE FILE {file} FROM {folder}')
            conn_tmp_ftp.delete(file)
        logging.info(f'FOLDER TO DELETE ARM FILE {folder}')
        conn_tmp_ftp.delete(f'{folder}.arm')
        conn_tmp_ftp.cwd('..')
        conn_tmp_ftp.rmd(f'{folder}')


@dag(**DAG_PARAMS)
def dwh_staffonly_aircraft_files():
    @task()
    def extract(**kwargs):
        create_temp_dir()

        ti: TaskInstance = kwargs['task_instance']
        date = (ti.execution_date - timedelta(days=1)).strftime('%Y%m%d')
        logging.info(f"Retrieving data from date: {date}")

        with get_src_ftp_hook().get_conn() as conn_src_ftp:
            conn_src_ftp.encoding = FTP_ENCODING
            download_files_from_source_ftp(conn_src_ftp, date)

        unzip_local_stored_files(LOCAL_STORAGE_PATH)

    @task()
    def transform():
        conn = get_ssh_hook().get_connection(get_ssh_hook().ssh_conn_id)
        conn_ssh = paramiko.SSHClient()
        conn_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn_ssh.connect(hostname=conn.host, username=conn.login, password=conn.password)

        with get_tmp_ftp_hook().get_conn() as conn_tmp_ftp:
            conn_tmp_ftp.encoding = FTP_ENCODING
            cleanup_non_loaded_folders(conn_tmp_ftp)
            upload_local_stored_files_to_temp_ftp(conn_tmp_ftp)
            decode_files_on_temp_ftp(conn_tmp_ftp, conn_ssh)

    @task()
    def load():
        with get_tmp_ftp_hook().get_conn() as conn_tmp_ftp:
            conn_tmp_ftp.encoding = FTP_ENCODING
            load_decrypted_data(conn_tmp_ftp)

    # Operators
    t_extract = extract()
    t_transform = transform()
    t_load = load()

    # Pipeline
    t_extract >> t_transform >> t_load


root = dwh_staffonly_aircraft_files()
