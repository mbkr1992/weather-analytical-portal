from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class PrecipitationHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.select_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'precipitation_qn_8, precipitation_r1, ' \
                            'precipitation_wrtr, precipitation_rs_ind)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        precipitation.measurement_category = 'hourly'
        precipitation.qn_8 = item.get('QN_8', None)

        precipitation.r1 = item.get('R1', None)
        if precipitation.r1 == '-999':
            precipitation.r1 = None

        precipitation.wrtr = item.get('WRTR', None)
        if precipitation.wrtr == '-999':
            precipitation.wrtr = None

        precipitation.rs_ind = item.get('RS_IND', None)
        if precipitation.rs_ind == '-999':
            precipitation.rs_ind = None

        return precipitation

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_8,
                item.r1,
                item.wrtr,
                item.rs_ind)

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
