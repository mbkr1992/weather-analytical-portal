from mapper_model.mapper import Mapper
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION

query_insert_air_temperature = 'INSERT INTO data_hub (' \
                               'station_id, measurement_date, measurement_category, ' \
                               'air_temperature_qn_9, air_temperature_tt_tu, air_temperature_rf_tu) ' \
                               'VALUES %s' \
                               'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING'

query_update_file_parsed_flag = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'


class AirTemperatureMapper(Mapper):

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
                extras.execute_values(curs, query_insert_air_temperature, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(query_update_file_parsed_flag, data)
