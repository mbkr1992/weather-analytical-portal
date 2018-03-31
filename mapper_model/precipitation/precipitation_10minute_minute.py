from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime

class Precipitation10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        interval = '10_minutes'

        list_of_items.append(create_rws_dau_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rws_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rws_ind_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_rws_dau_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RWS_DAU_10'
    name = 'Precipitation time'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='minute',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                         })


def create_rws_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RWS_10'
    name = 'Sum of precipitation'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn,
                             "code": code,
                         })


def create_rws_ind_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'RWS_IND_10'
    name = 'Precipitation indicator'
    value = get_value(item, code, None)
    return Precipitation(station_id=sid, date=date,
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
