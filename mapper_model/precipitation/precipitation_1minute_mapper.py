from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class Precipitation1MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.select_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'precipitation_qn, precipitation_rs_01,' \
                            'precipitation_rth_01, precipitation_rwh_01, precipitation_rs_ind_01) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d%H%M')
        precipitation.measurement_category = '1_minute'
        precipitation.qn = item.get('QN', None)

        precipitation.rs_01 = item.get('RS_01', None)
        if precipitation.rs_01 == '-999':
            precipitation.rs_01 = None

        precipitation.rth_01 = item.get('RTH_01', None)
        if precipitation.rth_01 == '-999':
            precipitation.rth_01 = None

        precipitation.rwh_01 = item.get('RWH_01', None)
        if precipitation.rwh_01 == '-999':
            precipitation.rwh_01 = None

        precipitation.rs_ind_01 = item.get('RS_IND_01', None)
        if precipitation.rs_ind_01 == '-999':
            precipitation.rs_ind_01 = None

        return precipitation

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.rs_01,
                item.rth_01,
                item.rwh_01,
                item.rs_ind_01)

    def insert_items(self, items):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item) for item in items]
                extras.execute_values(curs, self.select_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)