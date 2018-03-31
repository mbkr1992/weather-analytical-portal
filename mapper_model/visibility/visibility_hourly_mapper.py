from mapper_model.mapper import Mapper
from model.visibility import Visibility
from datetime import datetime


class VisibilityHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_v_vv(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_v_vv(sid, date, interval, item):
    qn = item.get('QN_8', None)
    code = 'V_VV'
    name = 'Visibility'
    value = get_value(item, code, None)
    return Visibility(station_id=sid, date=date,
                      interval=interval, name=name, unit='m',
                      value=value,
                      information={
                          "QN_8": qn,
                          "Measured": item.get('V_VV_I', None),
                          "description": 'P=human '
                                         'I=instrument ',
                      })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
