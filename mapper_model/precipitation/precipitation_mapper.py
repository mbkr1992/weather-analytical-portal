from mapper_model.mapper import Mapper
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION

query_insert_precipitation = 'INSERT INTO data_hub (' \
                             'station_id, measurement_date, measurement_category, ' \
                             'precipitation_qn, precipitation_qn_8, ' \
                             'precipitation_rs_01, precipitation_rs_ind_01, precipitation_rs_ind, ' \
                             'precipitation_rws_dau_10, precipitation_rws_10, precipitation_rws_ind_10, ' \
                             'precipitation_rth_01, precipitation_rwh_01, precipitation_wrtr, precipitation_r1) ' \
                             'VALUES %s' \
                             'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

query_update_file_parsed_flag = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'


class PrecipitationMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        pass

    def insert_items(self, items):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [item.to_tuple() for item in items]
                extras.execute_values(curs, query_insert_precipitation, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(query_update_file_parsed_flag, data)
