from mapper_model.mapper import Mapper
from model.soil_temperature import SoilTemperature
from datetime import datetime


class SoilTemperatureDailyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_v_te002m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te005m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te010m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te020m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_v_te050m(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_v_te002m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE002M'
    name = 'Daily soil temperature in 2 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te005m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE005M'
    name = 'Daily soil temperature in 5 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te010m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE010M'
    name = 'Daily soil temperature in 10 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te020m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE020M'
    name = 'Daily soil temperature in 20 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_v_te050m(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'V_TE050M'
    name = 'Daily soil temperature in 50 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]