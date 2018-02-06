from mapper_model.mapper import Mapper
from model.cloudiness import Cloudiness
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class CloudinessHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                                  'station_id, measurement_date, measurement_category, ' \
                                  'cloudiness_qn_8, cloudiness_v_n_i, cloudiness_v_n) ' \
                                  'VALUES %s' \
                                  'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING'

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        cloudiness = Cloudiness()
        cloudiness.station_id = item['STATIONS_ID']
        cloudiness.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        cloudiness.measurement_category = 'hourly'

        cloudiness.qn_8 = item.get('QN_8', None)
        if cloudiness.qn_8 == '-999':
            cloudiness.qn_8 = None

        cloudiness.v_n_i = item.get('V_N_I', None)
        if cloudiness.v_n_i == '-999':
            cloudiness.v_n_i = None

        cloudiness.v_n = item.get('V_N', None)
        if cloudiness.v_n == '-999':
            cloudiness.v_n = None

        return cloudiness

    @staticmethod
    def to_tuple(item: Cloudiness):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_8,
                item.v_n_i,
                item.v_n)

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
