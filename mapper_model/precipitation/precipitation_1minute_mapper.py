from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime


class Precipitation1MinuteMapper(Mapper):
    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d%H%M')
        interval = '1_minute'

        list_of_items.append(create_rs_01(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rth_01(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rwh_01(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rs_ind_01(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_rs_01(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RS_01'
    name = 'Rainfall'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                             "source": "DW",
                         })


def create_rth_01(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RTH_01'
    name = 'Rainfall drops'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                             "source": "DW",
                         })


def create_rwh_01(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RWH_01'
    name = 'Precipitation height'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                             "source": "DW",
                         })


def create_rs_ind_01(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RS_IND_01'
    name = 'Precipitation indicator'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='numeric code',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                             "source": "DW",
                         })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
