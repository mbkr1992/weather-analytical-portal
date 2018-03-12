from mapper_model.mapper import Mapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class SoilDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        list_of_items = []

        station_id = item['Stationsindex']
        date = datetime.strptime(item['Datum'], '%Y%m%d')
        interval = 'daily'

        list_of_items.append(create_vgsl(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_vpgb(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_vpgh(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ts05(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ts10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ts20(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ts50(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_ts100(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_zfumi(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf10(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf20(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf30(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf40(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf50(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bf60(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bfgsl(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_bfgls(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))
        return list_of_items

    @staticmethod
    def to_tuple(item, position):
        return (item.date,
                item.station_id,
                item.name,
                extras.Json(item.value),
                item.unit,
                item.interval,
                extras.Json(item.information),
                position)

    def insert_items(self, items, position=None):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item, position) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)


def create_vgsl(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'VGSL'
    name = 'Real evapotranspiration over grass and sandy loam (AMBAV)'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='mm',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_vpgb(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'VPGB'
    name = 'Potential evapotranspiration over grass (AMBAV)'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='mm',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_vpgh(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'VPGH'
    name = 'Potential evaporation over grass (Haude)'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='mm',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_ts05(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'TS05'
    name = 'Mean daily soil temperature in 5 cm depth for uncovered typical soil in 5 cm depth '\
           'mean daily soil',
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code
                           })


def create_ts10(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'TS10'
    name = 'Mean daily soil temperature in 5 cm depth for uncovered typical soil in 10 cm depth '\
           'mean daily soil',
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_ts20(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'TS20'
    name = 'Mean daily soil temperature in 5 cm depth for uncovered typical soil in 20 cm depth '\
           'mean daily soil'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_ts50(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'TS50'
    name = 'Mean daily soil temperature in 5 cm depth for uncovered typical soil in 50 cm depth '\
           'mean daily soil'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_ts100(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'TS100'
    name = 'Mean daily soil temperature in 5 cm depth for uncovered typical soil in 1 m depth '\
           'mean daily soil'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='°C',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_zfumi(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'ZFUMI'
    name = 'Frost depth at midday for uncovered soil'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='cm',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf10(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF10'
    name = 'Soil moisture under grass and sandy loam between 0 and 10 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf20(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF20'
    name = 'Soil moisture under grass and sandy loam between 10 and 20 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf30(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF30'
    name = 'Soil moisture under grass and sandy loam between 20 and 30 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf40(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF40'
    name = 'Soil moisture under grass and sandy loam between 30 and 40 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf50(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF50'
    name = 'Soil moisture under grass and sandy loam between 40 and 50 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bf60(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BF60'
    name = 'Soil moisture under grass and sandy loam between 50 and 60 cm depth in % plant '\
           'usable water'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bfgsl(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BFGSL'
    name = 'Soil moisture under grass and sandy loam up to 60 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
                           value=value,
                           information={
                               "QN": qn_2,
                               "code": code,
                           })


def create_bfgls(sid, date, interval, item):
    qn_2 = item.get('QN_2', None)
    code = 'BFGLS'
    name = 'Soil moisture under grass and loamy sand up to 60 cm depth'
    value = get_value(item, code, None)
    return SoilTemperature(station_id=sid, date=date,
                           interval=interval, name=name, unit='%nFK',
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

