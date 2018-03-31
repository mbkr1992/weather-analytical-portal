from mapper_model.mapper import Mapper
from model.wind import Wind
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION
from database_model import db_handler


class WindExtreme10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        interval = '10_minutes'

        list_of_items.append(create_fx_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fnx_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fmx_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_dx_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_fx_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'FX_10'
    name = 'Maximum wind speed'
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='m/s',
                value=value,
                information={
                    "QN": qn,
                    "code": code,
                })


def create_fnx_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'FNX_10'
    name = 'Minimum wind speed'
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='m/s',
                value=value,
                information={
                    "QN": qn,
                    "code": code,
                })


def create_fmx_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'FMX_10'
    name = 'Maximum mean wind speed '
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='~',
                value=value,
                information={
                    "QN": qn,
                    "code": code,
                })


def create_dx_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'DX_10'
    name = 'Maximum Wind direction'
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='~',
                value=value,
                information={
                    "QN": qn,
                    "code": code,
                })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
