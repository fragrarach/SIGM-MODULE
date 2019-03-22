import os
import psycopg2.extensions
import __main__


# Check whether app should reference dev or prod server/db
def dev_check():
    raw_filename = os.path.basename(__main__.__file__)
    removed_extension = raw_filename.split('.')[0]
    last_word = removed_extension.split('_')[-1]
    if last_word == 'DEV':
        return True
    else:
        return False


# Initialize production DB connection, listen cursor and query cursor
def sigm_conn(channel=None):
    global conn_sigm, sigm_query
    if dev_check():
        conn_sigm = psycopg2.connect("host='192.168.0.57' dbname='DEV' user='SIGM' port='5493'")
    else:
        conn_sigm = psycopg2.connect("host='192.168.0.250' dbname='QuatroAir' user='SIGM' port='5493'")
    conn_sigm.set_client_encoding("latin1")
    conn_sigm.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    if channel:
        sigm_listen = conn_sigm.cursor()
        sigm_listen.execute(f"LISTEN {channel};")
        print(f'Listening on {channel}')
    sigm_query = conn_sigm.cursor()

    return conn_sigm, sigm_query


# Initialize log DB connection, listen cursor and query cursor
def log_conn():
    global conn_log, log_query
    if dev_check():
        conn_log = psycopg2.connect("host='192.168.0.57' dbname='LOG' user='SIGM' port='5493'")
    else:
        conn_log = psycopg2.connect("host='192.168.0.250' dbname='LOG' user='SIGM' port='5493'")
    conn_log.set_client_encoding("latin1")
    conn_log.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    log_query = conn_log.cursor()

    return conn_log, log_query


def add_sql_files():
    script_path = os.path.realpath(__main__.__file__)
    parent = os.path.abspath(os.path.join(script_path, os.pardir))
    sql_folder = parent + '\\SUPPORTING FUNCTIONS'
    for file in os.listdir(sql_folder):
        if file.endswith(".sql"):
            file_path = sql_folder + f'\\{file}'
            with open(file_path, 'r') as sql_file:
                sigm_query.execute(sql_file.read())
                print(f'{file} added.')