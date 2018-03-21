from mapper_model.mapper import Mapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class SoilTemperatureDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_v_te002m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te005m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te010m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te020m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te050m(
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


def create_v_te002m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE002M'
    name = 'Daily soil temperature in 2 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te005m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE005M'
    name = 'Daily soil temperature in 5 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te010m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE010M'
    name = 'Daily soil temperature in 10 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te020m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE020M'
    name = 'Daily soil temperature in 20 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te050m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE050M'
    name = 'Daily soil temperature in 50 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]