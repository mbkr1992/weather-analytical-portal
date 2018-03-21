from mapper_model.mapper import Mapper
from model.cloudiness import Cloudiness
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class CloudinessHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        list_of_items = []

        station_id = item.get('STATIONS_ID', None)
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_vn(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items

    @staticmethod
    def to_tuple(item, position):
        return (item.date,
                item.station_id,
                item.name,
                extras.Json(item.value),
                item.unit,
                item.interval,
                extras.Json(item.information),
                position,
                item.source)

    def insert_items(self, items, position=None):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item, position) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)


def create_vn(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'V_N'
    name = 'Total cloud cover'
    value = get_value(item, code, None)
    return Cloudiness(station_id=sid, date=date,
                      interval=interval, name=name, unit=None,
                      value=value,
                      information={
                          "QN_8": qn_8,
                          "code": code,
                          "Measured": item.get('V_N_I', None),
                          "description": 'P=human '
                                         'I=instrument ',
                      })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
