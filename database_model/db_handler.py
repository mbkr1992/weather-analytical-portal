from postgis import Point
from postgis.psycopg import register
from psycopg2 import connect
from constants import constants
from model.solar import Solar

DBN = constants.DATABASE_CONNECTION

insert_station = 'INSERT ' \
                 'INTO station (id, name, position, state, from_date, to_date, height) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s);'

update_station = 'UPDATE station ' \
                 'SET id=(%s), name=(%s), position=(%s), state=(%s), from_date=(%s), to_date=(%s), height=(%s) ' \
                 'WHERE id=(%s);'

insert_solar = 'INSERT ' \
                  'INTO data_hub (station_id, measurement_date, solar_qn, solar_atmo, solar_fd, ' \
                  'solar_fg, solar_sd, solar_zenith, solar_measurement_date_local) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

insert_solar_dummy = 'INSERT ' \
                  'INTO data_hub (measurement_date) ' \
                  'VALUES (%s);'


def insert_stations(stations):
    print('Inserting stations')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            for s in stations:
                data = (s.id, s.name, Point(x=s.latitude, y=s.longitude, srid=4326),
                        s.state, s.from_date, s.to_date, s.height)
                curs.execute(insert_station, data)


def update_stations(stations):
    print('Updating stations')
    with connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            for s in stations:
                data = (s.id, s.name, Point(x=s.latitude, y=s.longitude, srid=4326),
                        s.state, s.from_date, s.to_date, s.height, s.id)
                curs.execute(update_station, data)


def insert_solar_data(items: [Solar]):
    print('Inserting into data hub')
    with connect(DBN) as conn:
        with conn.cursor() as curs:
            for s in items:
                data = (s.station_id, s.measurement_date, s.qn, s.atmo_radiation, s.fd_radiation, s.fg_radiation,
                        s.sd_radiation, s.zenith, s.measurement_date_local)
                curs.execute(insert_solar, data)
