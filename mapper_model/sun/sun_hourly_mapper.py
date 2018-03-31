from mapper_model.mapper import Mapper
from model.sun import Sun
from datetime import datetime


class SunHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []
        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_sd_so(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_sd_so(sid, date, interval, item):
    qn = item.get('QN_7', None)
    code = 'SD_SO'
    name = 'Hourly sunshine duration'
    value = get_value(item, code, None)
    return Sun(station_id=sid, date=date,
               interval=interval, name=name, unit='min',
               value=value,
               information={
                   "QN_7": qn,
                   "code": code,
               })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
