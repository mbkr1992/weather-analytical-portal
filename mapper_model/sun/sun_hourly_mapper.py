from mapper_model.mapper import Mapper
from model.sun import Sun
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class SunHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'sun_qn_7, sun_sd_so)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        sun = Sun()
        sun.station_id = item['STATIONS_ID']
        sun.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        sun.measurement_category = 'hourly'

        sun.qn_7 = item.get('QN_7', None)
        if sun.qn_7 == '-999':
            sun.qn_7 = None

        sun.sd_so = item.get('SD_SO', None)
        if sun.sd_so == '-999':
            sun.sd_so = None

        return sun

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_7,
                item.sd_so)

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

