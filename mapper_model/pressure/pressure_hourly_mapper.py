from mapper_model.mapper import Mapper
from model.pressure import Pressure
from datetime import datetime


class PressureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_p(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_p0(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_p(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'P'
    name = 'Mean sea level pressure'
    value = get_value(item, code, None)
    return Pressure(station_id=sid, date=date,
                    interval=interval, name=name, unit='hPA',
                    value=value,
                    information={
                        "QN": qn_8,
                        "code": code,
                    })


def create_p0(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'P0'
    name = 'Pressure at station height'
    value = get_value(item, code, None)
    return Pressure(station_id=sid, date=date,
                    interval=interval, name=name, unit='hPA',
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

