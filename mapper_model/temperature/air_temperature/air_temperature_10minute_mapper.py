from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class AirTemperature10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        interval = '10_minutes'

        list_of_items.append(create_pp(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tt(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tm5_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rf_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_td_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_pp(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'PP_10'
    name = 'Air pressure at station altitude'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='hpa',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def create_tt(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'TT_10'
    name = 'Air temperature in 2m height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='°C',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def create_tm5_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'TM5_10'
    name = 'Air temperature in 5cm height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='°C',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def create_rf_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RF_10'
    name = 'Relative humidity at 2m height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='%',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def create_td_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'TD_10'
    name = 'Dew point temperature in 2m height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='°C',
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
