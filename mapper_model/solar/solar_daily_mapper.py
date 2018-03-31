from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime


class SolarDailyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []
        station_id = item.get('STATIONS_ID', None)
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'hourly'

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

        return list_of_items


def create_atmo(sid, date, interval, item):
    qn = item.get('QN_592', None)
    code = 'ATMO_STRAHL'
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
    code = 'FD_STRAHL'
    name = 'Daily sum of diffuse solar radiation'
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
    code = 'FG_STRAHL'
    name = 'Daily sum of solar incoming radiation'
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
    code = 'SD_STRAHL'
    name = 'Daily sum of sunshine duration'
    value = get_value(item, code, None)
    return Solar(station_id=sid, date=date,
                 interval=interval, name=name, unit='min',
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
