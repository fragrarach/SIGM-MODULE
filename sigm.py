import os
import psycopg2.extensions
import __main__


# PostgreSQL DB connection configs
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


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
def sigm_connect(channel=None):
    global sigm_connection, sigm_db_cursor
    if dev_check():
        host = '192.168.0.57'
        dbname = 'DEV'
    else:
        host = '192.168.0.250'
        dbname = 'QuatroAir'
    sigm_connection = psycopg2.connect(f"host='{host}' dbname='{dbname}' user='SIGM' port='5493'")
    sigm_connection.set_client_encoding("latin1")
    sigm_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    if channel:
        sigm_listen = sigm_connection.cursor()
        sigm_listen.execute(f"LISTEN {channel};")
        print(f'Listening to channel {channel} on DB {dbname} at host {host}')
    sigm_db_cursor = sigm_connection.cursor()
    print(f'SIGM cursor open on DB {dbname} at host {host}')

    return sigm_connection, sigm_db_cursor


# Initialize log DB connection, listen cursor and query cursor
def log_connect():
    global log_connection, log_db_cursor
    if dev_check():
        host = '192.168.0.57'
    else:
        host = '192.168.0.250'
    log_connection = psycopg2.connect(f"host='{host}' dbname='LOG' user='SIGM' port='5493'")
    log_connection.set_client_encoding("latin1")
    log_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    log_db_cursor = log_connection.cursor()
    print(f'Log cursor open on DB LOG at host {host}')

    return log_connection, log_db_cursor


# Return containing folder path
def get_parent():
    script_path = os.path.realpath(__main__.__file__)
    parent_path = os.path.abspath(os.path.join(script_path, os.pardir))
    return parent_path


# Check for and add SQL files for main file
def add_sql_files():
    parent_path = get_parent()
    sql_folder = parent_path + '\\SUPPORTING FUNCTIONS'
    # TODO : Add DEV folder
    sigm_folder = sql_folder + '\\SIGM'
    log_folder = sql_folder + '\\LOG'
    sql_folders = [sigm_folder, log_folder]
    for folder in sql_folders:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                if file.endswith(".sql"):
                    file_path = folder + f'\\{file}'
                    with open(file_path, 'r') as sql_file:
                        if folder == sigm_folder:
                            sigm_db_cursor.execute(sql_file.read())
                        elif folder == log_folder:
                            log_db_cursor.execute(sql_file.read())
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


# Query SIGM database
def sigm_db_query(sql_exp):
    sigm_db_cursor.execute(sql_exp)
    result_set = sigm_db_cursor.fetchall()
    return result_set


# Query LOG database
def log_db_query(sql_exp):
    log_db_cursor.execute(sql_exp)
    result_set = log_db_cursor.fetchall()
    return result_set
