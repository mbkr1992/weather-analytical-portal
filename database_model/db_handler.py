from postgis import Point
from postgis.psycopg import register
from psycopg2 import connect, extras
from constants import constants
from model.solar import Solar
from model.file import File

DBN = constants.DATABASE_CONNECTION

query_insert_station = 'INSERT ' \
                       'INTO station (id, name, position, state, from_date, to_date, height) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s);'

query_insert_station_data = 'INSERT INTO data ' \
                            '(date, station_id, name, value, unit, time_category, information, position, source)' \
                            'VALUES %s' \
                            'ON CONFLICT (date, name, station_id, time_category) DO NOTHING '

query_update_file_is_parsed_flag = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'


query_insert_files = 'INSERT ' \
                     'INTO file_meta as file (filename, path, modify_date, priority) ' \
                     'VALUES %s ' \
                     'ON CONFLICT (path) DO UPDATE ' \
                     'SET modify_date=excluded.modify_date, is_downloaded=False, is_parsed=False ' \
                     'WHERE file.modify_date < excluded.modify_Date;'
def insert_files(files: [File]):
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            data = [file.to_tuple() for file in files]
            extras.execute_values(curs, query_insert_files, data, template=None, page_size=100)


query_select_path_of_non_downloaded_files_station = 'SELECT path FROM file_meta where is_downloaded=False AND path NOT LIKE {};'.format("'%.grib'")
def select_non_downloaded_files_station():
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_path_of_non_downloaded_files_station)
            rows = curs.fetchall()
            return [row[0] for row in rows]


query_select_path_of_non_downloaded_files_satellite = 'SELECT modify_date, path FROM file_meta where is_downloaded=False AND path LIKE {};'.format("'%.grib'")
def select_non_downloaded_files_satellite():
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_path_of_non_downloaded_files_satellite)
            rows = curs.fetchall()

            def to_file(row):
                date, path = row
                return File(path=path, modify_date=date)

            return [to_file(row) for row in rows]


query_select_path_of_non_parsed_files_station = "SELECT path FROM file_meta where is_parsed=False AND path NOT LIKE {} ORDER BY priority DESC".format("'%.grib'")
def select_non_parsed_files_station():
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_path_of_non_parsed_files_station)
            rows = curs.fetchall()
            return [row[0] for row in rows]


query_select_path_of_non_parsed_files_satellite = "SELECT path FROM file_meta where is_parsed=False AND path LIKE {} ORDER BY priority DESC".format("'%.grib'")
def select_non_parsed_files_satellite():
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_path_of_non_parsed_files_satellite)
            rows = curs.fetchall()
            return [row[0] for row in rows]


query_update_file_download_flag = 'UPDATE file_meta SET is_downloaded =(%s) WHERE path =(%s);'
def update_file_download_flag(path):
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            data = True, path
            curs.execute(query_update_file_download_flag, data)


query_select_station_with_id = "SELECT position FROM station where id = (%s)"
def get_station(id):
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            data = id,
            curs.execute(query_select_station_with_id, data)
            return curs.fetchone()