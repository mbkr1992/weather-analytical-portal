from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION
from database_model import db_handler


class PrecipitationHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_r1(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_wrtr(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rs_ind(
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


def create_r1(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    name = 'R1'
    value = get_value(item, name, None),
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='mm',
                         value=value,
                         information={
                             "QN": qn_8,
                             "description": 'hourly precipitation height',
                             "type": "precipitation",
                             "source": "DW",
                         })


def create_wrtr(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    name = 'wrtr'
    value = get_value(item, name, None),
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit='WR-code',
                         value=value,
                         information={
                             "QN": qn_8,
                             "description": 'form of precipitation',
                             "type": "precipitation",
                             "source": "DW",
                         })


def create_rs_ind(sid, date, interval, item):
    qn_8 = item.get('QN_8', None)
    name = 'RS_IND'
    value = get_value(item, name, None),
    return Precipitation(station_id=sid, date=date,
                         interval=interval, name=name, unit=None,
                         value=value,
                         information={
                             "QN": qn_8,
                             "description": '0 = no precipitation '
                                            '1 = precipitation has fallen ',
                             "type": "precipitation",
                             "source": "DW",
                         })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]

