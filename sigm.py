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
        host = '192.168.0.57'
        dbname = 'DEV'
    else:
        host = '192.168.0.250'
        dbname = 'QuatroAir'
    conn_sigm = psycopg2.connect(f"host='{host}' dbname='{dbname}' user='SIGM' port='5493'")
    conn_sigm.set_client_encoding("latin1")
    conn_sigm.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    if channel:
        sigm_listen = conn_sigm.cursor()
        sigm_listen.execute(f"LISTEN {channel};")
        print(f'Listening to channel {channel} on DB {dbname} at host {host}')
    sigm_query = conn_sigm.cursor()
    print(f'SIGM cursor open on DB {dbname} at host {host}')

    return conn_sigm, sigm_query


# Initialize log DB connection, listen cursor and query cursor
def log_conn():
    global conn_log, log_query
    if dev_check():
        host = '192.168.0.57'
    else:
        host = '192.168.0.250'
    conn_log = psycopg2.connect(f"host='{host}' dbname='LOG' user='SIGM' port='5493'")
    conn_log.set_client_encoding("latin1")
    conn_log.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    log_query = conn_log.cursor()
    print(f'Log cursor open on DB LOG at host {host}')

    return conn_log, log_query


# Check for and add SQL files for main file
def add_sql_files():
    script_path = os.path.realpath(__main__.__file__)
    parent = os.path.abspath(os.path.join(script_path, os.pardir))
    sql_folder = parent + '\\SUPPORTING FUNCTIONS'
    sigm_folder = sql_folder + '\\SIGM'
    log_folder = sql_folder + '\\LOG'
    sql_folders = [sigm_folder, log_folder]
    for folder in sql_folders:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                if file.endswith(".sql"):
                    file_path = folder + f'\\{file}'
                    with open(file_path, 'r') as sql_file:
                        sigm_query.execute(sql_file.read())
                        print(f'{file} added.')


# Convert tabular query result to list (2D array)
def tabular_data(result_set):
    lines = []
    for row in result_set:
        line = []
        for cell in row:
            if type(cell) == str:
                cell = cell.strip()
            line.append(cell)
        lines.append(line)
    return lines


# Convert scalar query result to singleton variable of any data type
def scalar_data(result_set):
    for row in result_set:
        for cell in row:
            if type(cell) == str:
                cell = cell.strip()
            return cell


# Query production database
def production_query(sql_exp):
    sigm_query.execute(sql_exp)
    result_set = sigm_query.fetchall()
    return result_set
