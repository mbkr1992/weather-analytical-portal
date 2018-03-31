from mapper_model.mapper import Mapper
from model.cloudiness import Cloudiness
from datetime import datetime


class CloudinessHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []

        station_id = item.get('STATIONS_ID', None)
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_vn(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_vn(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    code = 'V_N'
    name = 'Total cloud cover'
    value = get_value(item, code, None)
    return Cloudiness(station_id=sid, date=date,
                      interval=interval, name=name, unit=None,
                      value=value,
                      information={
                          "QN_8": qn_8,
                          "code": code,
                          "Measured": item.get('V_N_I', None),
                          "description": 'P=human '
                                         'I=instrument ',
                      })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
