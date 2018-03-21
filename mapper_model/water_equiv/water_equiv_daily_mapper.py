from mapper_model.mapper import Mapper
from model.water_equiv import WaterEquiv
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class WaterEquivDailyMapper(Mapper):

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

        list_of_items.append(create_ash_6(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_sh_tag(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_wash_6(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_waas_6(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))
        return list_of_items

    @staticmethod
    def is_valid(value):
        return value and value != '999'

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


def create_ash_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'ASH_6'
    name = 'Height of snow pack sample'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='cm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_sh_tag(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'SH_TAG'
    name = 'Total snow depth'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='cm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_wash_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'WASH_6'
    name = 'Total snow water equivalent'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='nm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_waas_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'WAAS_6'
    name = 'Total snow pack water equivalent'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='nm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
