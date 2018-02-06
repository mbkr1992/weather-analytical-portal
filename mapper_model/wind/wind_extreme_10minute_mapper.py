from mapper_model.mapper import Mapper
from model.wind import Wind
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class WindExtreme10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'wind_qn, wind_fx_10, wind_fnx_10, wind_fmx_10, wind_dx_10)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%S) WHERE path =(%S);'

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        wind.measurement_category = '10_minute'

        wind.qn = item.get('QN', None)
        if wind.qn == '-999':
            wind.qn = None

        wind.fx_10 = item.get('FX_10', None)
        if wind.fx_10 == '-999':
            wind.fx_10 = None

        wind.fnx_10 = item.get('FNX_10', None)
        if wind.fnx_10 == '-999':
            wind.fnx_10 = None

        wind.fmx_10 = item.get('FMX_10', None)
        if wind.fmx_10 == '-999':
            wind.fmx_10 = None

        wind.dx_10 = item.get('DX_10', None)
        if wind.dx_10 == '-999':
            wind.dx_10 = None

        return wind

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.fx_10,
                item.fnx_10,
                item.fmx_10,
                item.dx_10)

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
