import psycopg2
from Common import Constants
from postgis.psycopg import register
from postgis import Point

DBN = Constants.DATABASE_CONNECTION

insert_station = 'INSERT ' \
                 'INTO station (id, name, position, state, from_date, to_date, height) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s);'


def insert_stations(stations):
    with psycopg2.connect(DBN) as conn:
        register(connection=conn)
        with conn.cursor() as curs:
            print(conn);
            for s in stations:
                data = (s.id, s.name, Point(x=s.latitude, y=s.longitude, srid=4326), s.state, s.from_date, s.to_date, s.height)
                curs.execute(insert_station, data)