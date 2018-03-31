from mapper_model.mapper import Mapper
from model.wind import Wind
from datetime import datetime


class Wind10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        interval = '10_minutes'

        list_of_items.append(create_ff_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_dd_10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_ff_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'FF_10'
    name = 'Mean wind speed'
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='m/s',
                value=value,
                information={
                    "QN": qn,
                    "code": code,
                })


def create_dd_10(sid, date, interval, item):
    qn = item.get('QN', None)
    code = 'DD_10'
    name = 'Mean wind direction'
    value = get_value(item, code, None)
    return Wind(station_id=sid, date=date,
                interval=interval, name=name, unit='Degree',
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
