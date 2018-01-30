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

query_update_station = 'UPDATE station ' \
                       'SET id=(%s), name=(%s), position=(%s), state=(%s), from_date=(%s), to_date=(%s), height=(%s) ' \
                       'WHERE id=(%s);'

query_insert_solar = 'INSERT ' \
                     'INTO data_hub (station_id, measurement_date, solar_qn, solar_atmo, solar_fd, ' \
                     'solar_fg, solar_sd, solar_zenith, solar_measurement_date_local) ' \
                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

query_insert_files = 'INSERT ' \
                     'INTO file_meta (path, modify_date) ' \
                     'VALUES %s ' \
                     'ON CONFLICT (path) DO UPDATE ' \
                     'SET modify_date = file_meta.modify_date;'

query_select_files_part_one = 'DECLARE super_cursor BINARY CURSOR FOR ' \
                              'SELECT path FROM file_meta WHERE is_downloaded = {0};'.format(False)

query_select_files_part_two = 'FETCH 1000 FROM super_cursor;'

query_select_files_simple = 'SELECT path FROM file_meta LIMIT 5;'

query_update_files = 'UPDATE file_meta SET is_downloaded =(%s) WHERE path =(%s);'


def insert_files(files: [File]):
    print('Inserting files')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            data = [file.to_tuple() for file in files]
            extras.execute_values(curs, query_insert_files, data, template=None, page_size=100)


def select_files(batch_operation):
    print('Fetching files', batch_operation)
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_files_part_one)
            while True:
                curs.execute(query_select_files_part_two)
                rows = curs.fetchall()

                print('Fetching files', rows)

                if not rows:
                    break

                for row in rows:
                    batch_operation(row[0])


def select_files_simple():
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            curs.execute(query_select_files_part_one)
            while True:
                curs.execute(query_select_files_simple)
                rows = curs.fetchall()
                return [row[0] for row in rows]


def update_file(path):
    print('Updating files')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            data = True, path
            curs.execute(query_update_files, data)


def insert_stations(stations):
    print('Inserting stations')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            for s in stations:
                data = (s.id, s.name, Point(x=s.latitude, y=s.longitude, srid=4326),
                        s.state, s.from_date, s.to_date, s.height)
                curs.execute(query_insert_station, data)


def update_stations(stations):
    print('Updating stations')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            for s in stations:
                data = (s.id, s.name, Point(x=s.latitude, y=s.longitude, srid=4326),
                        s.state, s.from_date, s.to_date, s.height, s.id)
                curs.execute(query_update_station, data)


def insert_solar_data(items: [Solar]):
    print('Inserting into data hub')
    with connect(DBN) as conn:
        with conn.cursor() as curs:
            for s in items:
                data = (s.station_id, s.measurement_date, s.qn, s.atmo_radiation, s.fd_radiation, s.fg_radiation,
                        s.sd_radiation, s.zenith, s.measurement_date_local)
                curs.execute(query_insert_solar, data)
