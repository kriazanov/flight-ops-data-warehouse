"""
**Description**: Skytrac mail information about take-off and landing
**Target**: `raw_skytrac_mails`
**Call**: `sf_load_skytrac_takeoff_landing_data`
"""

import email
import math
import logging
import imaplib

from datetime import datetime, timedelta
from decimal import Decimal
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.imap.hooks.imap import ImapHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'airflow',
    'execution_timeout': timedelta(hours=1),
}

DAG_PARAMS = {
    'default_args': DEFAULT_ARGS,
    'start_date': datetime(2023, 1, 24),
    'schedule_interval': '0 5 * * *',
    'description': 'Skytrac mail information about take-off and landingu',
    'tags': ['dwh', 'skytrac', 'imap', 'email'],
    'max_active_runs': 1
}

SRC_HOOK_CONN_NAME = 'conn_imap_skytrac'
TGT_HOOK_CONN_NAME = 'conn_dwh'

IMAP_MAIL_FILTER_SEEN = '(SEEN)'
IMAP_MAIL_FILTER_UNSEEN = '(UNSEEN)'


def get_source_hook_imap():
    return ImapHook(SRC_HOOK_CONN_NAME)


def get_target_hook_dwh():
    return PostgresHook(TGT_HOOK_CONN_NAME)


def get_hostname_imap():
    return get_source_hook_imap().get_connection(SRC_HOOK_CONN_NAME).host


def get_username_imap():
    return get_source_hook_imap().get_connection(SRC_HOOK_CONN_NAME).login


def get_password_imap():
    return get_source_hook_imap().get_connection(SRC_HOOK_CONN_NAME).password



def get_timestamp(raw_datetime):
    raw_timestamp = email.utils.parsedate_tz(raw_datetime)
    year, month, day, hour, minute, second = raw_timestamp[:6]
    return datetime(year, month, day, hour, minute, second).isoformat()


def get_geocoordinate_orientation(char):
    result = ''
    if char in ('S', 'W'):
        result = '-'
    return result


def get_decoded_coordinates_newest_format(raw):
    decoded_lat = raw.split(' ')
    return get_geocoordinate_orientation(decoded_lat[0]) + decoded_lat[1] + '°' + decoded_lat[3] + "'"


def get_decoded_coordinates_new_format(raw):
    decoded_lat = raw.split(' ')
    degrees = Decimal(decoded_lat[1])
    minutes = Decimal(decoded_lat[3])
    deg_value = round(degrees * Decimal('1.0') + (minutes / Decimal('60.0')), 6)
    return get_geocoordinate_orientation(decoded_lat[0]) + str(deg_value)


def get_decoded_coordinates_historic_format(raw):
    decoded_val = raw.split(' ')
    degrees = Decimal(decoded_val[1])
    minutes = math.floor(Decimal(decoded_val[3]))
    remaining = Decimal(decoded_val[3]) - (minutes * Decimal('1.0'))
    seconds = remaining * Decimal('60.0')
    seconds = round(seconds * Decimal('10.0') / Decimal('10.0'), 1)
    return str(degrees) + '°' + str(minutes) + "'" + str(seconds) + '"'


def get_mails(imap, mail_filter):
    status, select_data = imap.select('INBOX')
    status, search_data = imap.search(None, mail_filter)
    return search_data


def inbox_cleanup(imap):
    search_data = get_mails(imap, IMAP_MAIL_FILTER_SEEN)

    for msg_id in search_data[0].split():
        status, msg_data = imap.fetch(msg_id, '(RFC822)')
        imap.store(msg_id, '+FLAGS', '\\Deleted')

    imap.expunge()


def set_mails_unseen(imap):
    search_data = get_mails(imap, IMAP_MAIL_FILTER_SEEN)
    for msg_id in search_data[0].split():
        imap.store(msg_id, '-FLAGS', '(\SEEN)')


def skip_check(message_count):
    if message_count == 0:
        raise AirflowSkipException


@dag(**DAG_PARAMS)
def dwh_skytrac_emails():
    @task()
    def extract():
        imap = imaplib.IMAP4(get_hostname_imap())
        imap.login(get_username_imap(), get_password_imap())
        set_mails_unseen(imap)  # If previous run fails, emails will be marked as unread and loaded again

        search_data = get_mails(imap, IMAP_MAIL_FILTER_UNSEEN)

        query = '''
        insert into dwh_staging.raw_skytrac_mails(
            mail_mailbox_id,
            mail_date,
            mail_from,
            mail_to,
            mail_subject,
            mail_body,
            raw_board,
            raw_takeoff_landing_kind,
            raw_datetime,
            raw_lat,
            raw_long,
            raw_skytrac_approx_position,
            raw_alt,
            raw_speed,
            raw_track,
            decoded_iso_datetime_utc,
            decoded_lat_newest_format,
            decoded_lat_new_format,
            decoded_lat_historic_format,
            decoded_long_newest_format,
            decoded_long_new_format,
            decoded_long_historic_format,
            decoded_alt_value,
            decoded_alt_mu,
            decoded_speed_value,
            decoded_speed_mu,
            date_created,
            date_processed) VALUES\n
        '''

        message_count = len(search_data[0].split())
        logging.info(f"MESSAGE COUNT: {message_count}")
        skip_check(message_count)

        for msg_id in search_data[0].split():
            status, msg_data = imap.fetch(msg_id, '(RFC822)')
            msg_raw = msg_data[0][1]
            msg = email.message_from_bytes(msg_raw, _class=email.message.EmailMessage)

            # mail meta data
            mail_mailbox_id = str(msg_id)
            mail_date = get_timestamp(msg['Date'])
            mail_from = msg['From']
            mail_to = msg['To']
            mail_subject = msg['Subject']
            mail_body = str(msg.get_payload(decode=True), encoding="utf-8")
            mail_lines = mail_body.split("\r\n")

            # raw data
            raw_board = mail_subject.replace('Message concerning ', '').split(' ')[0]
            raw_takeoff_landing_kind = mail_lines[0]
            raw_datetime = mail_lines[4].replace('Time: ', '')
            raw_lat = mail_lines[5].replace('Lat: ', '')
            raw_long = mail_lines[6].replace('Long: ', '')
            raw_alt = mail_lines[8].replace('Alt: ', '')
            raw_speed = mail_lines[9].replace('Speed: ', '')
            raw_track = mail_lines[10].replace('Track: ', '')
            if mail_lines[5] != '<NO GPS>':
                raw_skytrac_approx_position = mail_lines[7]
            else:
                raw_skytrac_approx_position = '<NO DATA>'

            # decoded data
            if mail_lines[5] != 'Lat: <NO GPS>':
                decoded_iso_datetime_utc = get_timestamp(raw_datetime)
                decoded_lat_newest_format = get_decoded_coordinates_newest_format(raw_lat)
                decoded_lat_new_format = get_decoded_coordinates_new_format(raw_lat)
                decoded_lat_historic_format = get_decoded_coordinates_historic_format(raw_lat)
                decoded_long_newest_format = get_decoded_coordinates_newest_format(raw_long)
                decoded_long_new_format = get_decoded_coordinates_new_format(raw_long)
                decoded_long_historic_format = get_decoded_coordinates_historic_format(raw_long)
                decoded_alt = raw_alt.split(' ')
                decoded_alt_value = decoded_alt[0]
                decoded_alt_mu = decoded_alt[1]
                decoded_speed = raw_speed.split(' ')
                decoded_speed_value = decoded_speed[0]
                decoded_speed_mu = decoded_speed[1]
            else:
                decoded_iso_datetime_utc = get_timestamp(raw_datetime)
                decoded_lat_newest_format = ''
                decoded_lat_new_format = ''
                decoded_lat_historic_format = ''
                decoded_long_newest_format = ''
                decoded_long_new_format = ''
                decoded_long_historic_format = ''
                decoded_alt_value = ''
                decoded_alt_mu = ''
                decoded_speed = raw_speed.split(' ')
                decoded_speed_value = decoded_speed[0]
                decoded_speed_mu = decoded_speed[1]

            # appending query
            mail_mailbox_id = mail_mailbox_id.replace("'", "''")
            decoded_lat_newest_format = decoded_lat_newest_format.replace("'", "''")
            decoded_lat_historic_format = decoded_lat_historic_format.replace("'", "''")
            decoded_long_newest_format = decoded_long_newest_format.replace("'", "''")
            decoded_long_historic_format = decoded_long_historic_format.replace("'", "''")
            query += f"('{mail_mailbox_id}', '{mail_date}', '{mail_from}', '{mail_to}', '{mail_subject}', '{mail_body}', " \
                     f"'{raw_board}', '{raw_takeoff_landing_kind}', '{raw_datetime}', '{raw_lat}', '{raw_long}', '{raw_skytrac_approx_position}', '{raw_alt}', '{raw_speed}', '{raw_track}', " \
                     f"'{decoded_iso_datetime_utc}', '{decoded_lat_newest_format}', '{decoded_lat_new_format}', '{decoded_lat_historic_format}', '{decoded_long_newest_format}', '{decoded_long_new_format}', '{decoded_long_historic_format}', '{decoded_alt_value}', '{decoded_alt_mu}', '{decoded_speed_value}', '{decoded_speed_mu}', now(), null),\n"

        query = query.rstrip(',\n')
        get_target_hook_dwh().run(query)

        inbox_cleanup(imap)  # Cleanup just fetched mails in this run
        imap.close()
        imap.logout()

    @task()
    def transform():
        get_target_hook_dwh().run(f'''select dwh_staging.sf_transform_skytrac_takeoff_landing_data();''')

    @task()
    def load():
        get_target_hook_dwh().run(f'''select dwh_staging.sf_load_skytrac_takeoff_landing_data();''')

    # Operators
    t_extract = extract()
    t_transform = transform()
    t_load = load()

    # Pipeline
    t_extract >> t_transform >> t_load


root = dwh_skytrac_emails()
root.doc_md = __doc__
