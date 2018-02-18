from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class AirTemperatureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_tt_tu(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rf_tu(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items

    @staticmethod
    def to_tuple(item):
        return (item.name,
                extras.Json(item.value),
                item.date,
                item.station_id,
                item.interval,
                extras.Json(item.information))

    def insert_items(self, items):
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


def create_tt_tu(sid, date, interval, item):
    qn = item.get('QN_9', None)
    name = 'TT_TU'
    value = get_value(item, name, None),
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='°C',
                          value=value,
                          information={
                              "QN": qn,
                              "description": '2m air temperature',
                              "type": "Air",
                              "source": "DW",
                          })


def create_rf_tu(sid, date, interval, item):
    qn = item.get('QN_9', None)
    name = 'RF_TU'
    value = get_value(item, name, None),
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='%',
                          value=value,
                          information={
                              "QN": qn,
                              "description": '2m relative humidity',
                              "type": "Air",
                              "source": "DW",
                          })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
