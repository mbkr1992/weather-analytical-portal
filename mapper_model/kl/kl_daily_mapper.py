from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime


class KlDailyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_fx(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_fm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rsk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rskf(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_sdk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_shk_tag(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_nm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_vpm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_pm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tmk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_upm(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_txk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tnk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_tgk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_fx(sid, date, interval, item):
    qn_3 = item.get('QN_3', None)
    code = 'FX'
    name = 'Daily maximum of wind gust'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='m/s',
              value=value,
              information={
                  "QN_3": qn_3,
                  "code": code,
              })


def create_fm(sid, date, interval, item):
    qn_3 = item.get('QN_3', None)
    code = 'FM'
    name = 'Daily mean of wind velocity'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='m/s',
              value=value,
              information={
                  "QN_3": qn_3,
                  "code": code,
              })


def create_rsk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'RSK'
    name = 'Daily precipitation height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='mm',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_rskf(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'RSKF'
    name = 'Precipitation form'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='~',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_sdk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'SDK'
    name = 'Daily sunshine duration'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='h',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_shk_tag(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'SHK_TAG'
    name = 'Daily snow depth'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='cm',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_nm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'NM'
    name = 'Daily mean of cloud cover'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='1/8',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_vpm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'VPM'
    name = 'Daily mean of vapor pressure'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='hPa',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_pm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'PM'
    name = 'Daily mean of pressure'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='hPa',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tmk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TMK'
    name = 'Daily mean of temperature'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_upm(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'UPM'
    name = 'Daily mean of relative humidity'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='%',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_txk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TXK'
    name = 'Daily maximum of temperature at 2m height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tnk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TNK'
    name = 'Daily minimum of temperature at 2m height'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def create_tgk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    code = 'TGK'
    name = 'Daily minimum of air temperature at 5cm above ground'
    value = get_value(item, code, None)
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "code": code,
              })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]