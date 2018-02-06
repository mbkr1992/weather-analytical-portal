from mapper_model.visibility.visibility_mapper import VisibilityMapper
from model.wind import Wind
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class VisibilityHourlyMapper(VisibilityMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'visibility_qn_8, visibility_v_vv_i, visibility_v_vv)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%S) WHERE path =(%S);'

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        wind.measurement_category = 'hourly'

        wind.qn_8 = item.get('QN_8', None)
        if wind.qn_8 == '-999':
            wind.qn_8 = None

        wind.v_vv_i = item.get('V_VV_I', None)
        if wind.v_vv_i == '-999':
            wind.v_vv_i = None

        wind.v_vv = item.get('V_VV', None)
        if wind.v_vv == '-999':
            wind.v_vv = None

        return wind

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_8,
                item.v_vv_i,
                item.v_vv)

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

