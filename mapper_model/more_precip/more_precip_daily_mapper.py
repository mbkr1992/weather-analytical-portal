from mapper_model.mapper import Mapper
from model.more_precip import MorePrecip
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register


class MorePrecipDailyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_rs(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rsf(
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

        return list_of_items


def create_rs(sid, date, interval, item):
    qn_6 = item.get('QN_6', None)
    code = 'RS'
    name = 'Daily precipitation height'
    value = get_value(item, code, None)
    return MorePrecip(station_id=sid, date=date,
                      interval=interval, name=name, unit='mm',
                      value=value,
                      information={
                          "QN_6": qn_6,
                          "code": code,
                      })


def create_rsf(sid, date, interval, item):
    qn_6 = item.get('QN_6', None)
    code = 'RSF'
    name = 'Precipitation form'
    value = get_value(item, code, None)
    return MorePrecip(station_id=sid, date=date,
                      interval=interval, name=name, unit='~',
                      value=value,
                      information={
                          "QN_6": qn_6,
                          "code": 'code',
                      })


def create_sh_tag(sid, date, interval, item):
    qn_6 = item.get('QN_6', None)
    code = 'SH_TAG'
    name = 'Daily height of snow pack'
    value = get_value(item, code, None)
    return MorePrecip(station_id=sid, date=date,
                      interval=interval, name=name, unit='cm',
                      value=value,
                      information={
                          "QN_6": qn_6,
                          "code": code,
                      })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
