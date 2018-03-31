from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION
from database_model import db_handler


class PrecipitationHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_r1(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_wrtr(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rs_ind(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_r1(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'R1'
    name = 'Hourly precipitation height'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn_8,
                             "code": code,
                         })


def create_wrtr(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'wrtr'
    name = 'Form of precipitation'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='WR-code',
                         value=value,
                         information={
                             "QN": qn_8,
                             "code": code,
                         })


def create_rs_ind(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'RS_IND'
    name = 'Precipitation fallen'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='~',
                         value=value,
                         information={
                             "QN": qn_8,
                             "code": code,
                         })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]

