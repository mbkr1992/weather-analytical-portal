from mapper_model.mapper import Mapper
from model.water_equiv import WaterEquiv
from datetime import datetime


class WaterEquivDailyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_ash_6(
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

        list_of_items.append(create_wash_6(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_waas_6(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))
        return list_of_items

    @staticmethod
    def is_valid(value):
        return value and value != '999'


def create_ash_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'ASH_6'
    name = 'Height of snow pack sample'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='cm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_sh_tag(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'SH_TAG'
    name = 'Total snow depth'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='cm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_wash_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'WASH_6'
    name = 'Total snow water equivalent'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='nm',
                      value=value,
                      information={
                          "QN_8": qn,
                          "code": code,
                      })


def create_waas_6(sid, date, interval, item):
    qn = item.get('QN_6', None)
    code = 'WAAS_6'
    name = 'Total snow pack water equivalent'
    value = get_value(item, code, None)
    return WaterEquiv(station_id=sid, date=date,
                      interval=interval, name=name, unit='nm',
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
