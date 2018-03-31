from mapper_model.mapper import Mapper
from model.station import Station
from psycopg2 import connect, extras
from datetime import datetime
from postgis.psycopg import register
from postgis import Point
from constants.constants import DATABASE_CONNECTION


class StationMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = 'INSERT ' \
                            'INTO station (id, name, state, from_date, to_date, height, position, latitude, longitude) ' \
                            'VALUES %s' \
                            'ON CONFLICT (id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item):
        station = Station()
        station.id = item.get('Stations_id', None) or item.get('ST_ID', None)

        station.name = item.get('Stationsname', None)

        station.latitude = item.get('geoBreite', None) or item.get('geo_Breite', None)
        station.longitude = item.get('geoLaenge', None) or item.get('geo_Laenge', None)
        station.position = Point(x=station.latitude, y=station.longitude, srid=4326)

        station.state = item.get('Bundesland', None) or item.get('Bundeslandname', None)

        from_date = item.get('von_datum', None) or item.get('von', None)
        to_date = item.get('bis_datum', None) or item.get('von', None)
        station.from_date = datetime.strptime(from_date, '%Y%m%d')
        station.to_date = datetime.strptime(to_date, '%Y%m%d')

        station.height = item.get('Stationshoehe', None) or item.get('ST_Hoehe', None)
        return station

    @staticmethod
    def to_tuple(item):
        return (item.id,
                item.name,
                item.state,
                item.from_date,
                item.to_date,
                item.height,
                item.position,
                item.latitude,
                item.longitude)

    def insert_items(self, items, position=None):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)

