from postgis import Point
from postgis.psycopg import register
from psycopg2 import connect

from constants import constants

DBN = constants.DATABASE_CONNECTION

insert_station = 'INSERT ' \
                 'INTO station (id, name, position, state, from_date, to_date, height) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s);'

update_station = 'UPDATE station ' \
                 'SET id=(%s), name=(%s), position=(%s), state=(%s), from_date=(%s), to_date=(%s), height=(%s) ' \
                 'WHERE id=(%s);'


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

