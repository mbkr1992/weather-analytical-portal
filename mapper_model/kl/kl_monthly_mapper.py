from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class KlMonthlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d')
        interval = 'monthly'

        list_of_items.append(create_mo_n(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_tt(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_tx(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_tn(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_sd_s(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_fk(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mx_tx(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mx_fx(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mx_tn(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mo_rr(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_mx_rs(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items

    @staticmethod
    def to_tuple(item):
        return (item.name,
                extras.Json(item.value),
                item.date,
                item.station_id,
                item.interval,
                extras.Json(item.information))

    def insert_items(self, items):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)


def create_mo_n(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_N'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='1/8',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly mean of cloud cover',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_tt(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_TT'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly mean of daily temperature means in 2m',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_tx(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_TX'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly mean of daily temperature maxima in 2m',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_tn(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_TN'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit=None,
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly mean of daily temperature minima in 2m',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_sd_s(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_SD_S'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='h',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly sum of sunshine duration',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_fk(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MO_FK'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='Bft',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly mean of daily wind speed',
                  "type": "kl",
                  "source": "DW",
              })


def create_mx_tx(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MX_TX'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly maximum of daily temperature maxima in 2m height',
                  "type": "kl",
                  "source": "DW",
              })


def create_mx_fx(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MX_FX'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='m/s',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly maximum of daily wind speed',
                  "type": "kl",
                  "source": "DW",
              })


def create_mx_tn(sid, date, interval, item):
    qn_4 = item.get('QN_4', None)
    name = 'MX_TN'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='°C',
              value=value,
              information={
                  "QN_4": qn_4,
                  "description": 'monthly minimum of daily temperature minima in 2m height',
                  "type": "kl",
                  "source": "DW",
              })


def create_mo_rr(sid, date, interval, item):
    qn_6 = item.get('QN_6', None)
    name = 'MO_RR'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='mm',
              value=value,
              information={
                  "QN_4": qn_6,
                  "description": 'monthly sum of precipitation height',
                  "type": "kl",
                  "source": "DW",
              })


def create_mx_rs(sid, date, interval, item):
    qn_6 = item.get('QN_6', None)
    name = 'MX_RS'
    value = get_value(item, name, None),
    return Kl(station_id=sid, date=date,
              interval=interval, name=name, unit='mm',
              value=value,
              information={
                  "QN_4": qn_6,
                  "description": 'monthly maximum of daily',
                  "type": "kl",
                  "source": "DW",
              })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
