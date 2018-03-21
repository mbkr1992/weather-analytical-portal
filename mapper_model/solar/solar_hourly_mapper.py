from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION
from database_model import db_handler


class SolarHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H:%M')
        interval = 'daily'

        list_of_items.append(create_atmo(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fd(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fg(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_sd(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_zenit(
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


def create_atmo(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'ATMO_LBERG'
    name = 'Longwave downward radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm^2',
                 value=value,
                 information={
                     "QN_592": qn,
                     "code": code,
                 })


def create_fd(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'FD_LBERG'
    name = 'Hourly sum of diffuse solar radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm^2',
                 value=value,
                 information={
                     "QN_592": qn,
                     "code": code,
                 })


def create_fg(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'FG_LBERG'
    name = 'Hourly sum of solar incoming radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm^2',
                 value=value,
                 information={
                     "QN_592": qn,
                     "code": code,
                 })


def create_sd(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'SD_LBERG'
    name = 'Hourly sum of sunshine duration'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='min',
                 value=value,
                 information={
                     "QN_592": qn,
                     "code": code,
                 })


def create_zenit(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'ZENIT'
    name = 'Solar zenith angle at mid of interval',
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='degree',
                 value=value,
                 information={
                     "QN_592": qn,
                     "code": code,
                 })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
