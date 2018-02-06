from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class Precipitation10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.select_query = 'INSERT INTO data_hub (' \
                                     'station_id, measurement_date, measurement_category, ' \
                                     'precipitation_qn, ' \
                                     'precipitation_rws_dau_10, precipitation_rws_10, precipitation_rws_ind_10) ' \
                                     'VALUES %s' \
                                     'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        precipitation.measurement_category = '10_minutes'
        precipitation.qn = item.get('QN', None)

        precipitation.rws_dau_10 = item.get('RWS_DAU_10', None)
        if precipitation.rws_dau_10 == '-999':
            precipitation.rws_dau_10 = None

        precipitation.rws_10 = item.get('RWS_10', None)
        if precipitation.rws_10 == '-999':
            precipitation.rws_10 = None

        precipitation.rws_ind_10 = item.get('RWS_IND_10', None)
        if precipitation.rws_ind_10 == '-999':
            precipitation.rws_ind_10 = None

        return precipitation

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.rws_dau_10,
                item.rws_10,
                item.rws_ind_10)

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
