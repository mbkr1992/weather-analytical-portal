from mapper_model.mapper import Mapper
from model.pressure import Pressure
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class PressureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'pressure_qn_8, pressure_p, pressure_p0)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%S) WHERE path =(%S);'

    def map(self, item={}):
        pressure = Pressure()
        pressure.station_id = item['STATIONS_ID']
        pressure.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        pressure.measurement_category = 'hourly'

        pressure.qn_8 = item.get('QN_8', None)
        if pressure.qn_8 == '-999':
            pressure.qn_8 = None

        pressure.p = item.get('P', None)
        if pressure.p == '-999':
            pressure.p = None

        pressure.p0 = item.get('P0', None)
        if pressure.p0 == '-999':
            pressure.p0 = None

        return pressure

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_8,
                item.p,
                item.p0)

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
