from abc import ABC, abstractmethod
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants import constants

query_insert_station_data = 'INSERT INTO data ' \
                            '(date, station_id, name, value, unit, time_category, information, source, position, latitude, longitude)' \
                            'VALUES %s' \
                            'ON CONFLICT (date, name, station_id, time_category) DO NOTHING '

query_update_file_is_parsed_flag = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

class Mapper(ABC):
    def __init__(self):
        super().__init__()
        self.dbc = constants.DATABASE_CONNECTION
        self.insert_query = query_insert_station_data
        self.update_query = query_update_file_is_parsed_flag

    @abstractmethod
    def map(self, item):
        pass

    @staticmethod
    def to_tuple(item, station=None):
        return (item.date,
                item.station_id,
                item.name,
                extras.Json(item.value),
                item.unit,
                item.interval,
                extras.Json(item.information),
                item.source,
                station.position,
                station.latitude,
                station.longitude)

    def insert_items(self, items, station=None):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item, station) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)