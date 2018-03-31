from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime


class Solar10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []
        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        interval = '10_minutes'

        list_of_items.append(create_ds_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_gs_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_sd_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ls_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_ds_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'DS_10'
    name = 'Diffuse sky radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm',
                 value=value,
                 information={
                     "QN_8": qn,
                     "code": code,
                 })


def create_gs_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'GS_10'
    name = 'Global radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm',
                 value=value,
                 information={
                     "QN_8": qn,
                     "code": code,
                 })


def create_sd_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'SD_10'
    name = 'Sunshine duration'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='hour',
                 value=value,
                 information={
                     "QN_8": qn,
                     "code": code,
                 })


def create_ls_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'LS_10'
    name = 'Long-wave radiation'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='J/cm',
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
