from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class KlDailyMapper(Mapper):

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

        list_of_items.append(create_fx(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rsk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rskf(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_sdk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_shk_tag(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_nm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_vpm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_pm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tmk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_upm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_txk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tnk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tgk(
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


def create_fx(sid, date, interval, item):
    qn_3 = item.get('QN_3', None)
    code = 'FX'
    name = 'Daily maximum of wind gust'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='m/s',
              value=value,
              information={
                  "QN_3": qn_3,
                  "code": code,
              })


def create_fm(sid, date, interval, item):
    qn_3 = item.get('QN_3', None)
    code = 'FM'
    name = 'Daily mean of wind velocity'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='m/s',
              value=value,
              information={
                  "QN_3": qn_3,
                  "code": code,
              })


def create_rsk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'RSK'
    name = 'Daily precipitation height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='mm',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_rskf(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'RSKF'
    name = 'Precipitation form'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='~',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_sdk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'SDK'
    name = 'Daily sunshine duration'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='h',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_shk_tag(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'SHK_TAG'
    name = 'Daily snow depth'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='cm',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_nm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'NM'
    name = 'Daily mean of cloud cover'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='1/8',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_vpm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'VPM'
    name = 'Daily mean of vapor pressure'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='hPa',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_pm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'PM'
    name = 'Daily mean of pressure'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='hPa',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tmk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TMK'
    name = 'Daily mean of temperature'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_upm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'UPM'
    name = 'Daily mean of relative humidity'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='%',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_txk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TXK'
    name = 'Daily maximum of temperature at 2m height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tnk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TNK'
    name = 'Daily minimum of temperature at 2m height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tgk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TGK'
    name = 'Daily minimum of air temperature at 5cm above ground'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]