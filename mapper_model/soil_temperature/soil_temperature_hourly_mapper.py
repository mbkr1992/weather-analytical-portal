from mapper_model.soil_temperature.soil_temperature_mapper import SoilTemperatureMapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class SoilTemperatureHourlyMapper(SoilTemperatureMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'soil_temperature_qn_2, soil_temperature_v_te002m, ' \
                            'soil_temperature_v_te005m, soil_temperature_v_te010m, soil_temperature_v_te020m, ' \
                            'soil_temperature_v_te050m, soil_temperature_vte100m) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%S) WHERE path =(%S);'

    def map(self, item={}):
        soil_temperature = SoilTemperature()
        soil_temperature.station_id = item['STATIONS_ID']
        soil_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        soil_temperature.measurement_category = 'daily'

        soil_temperature.qn_2 = item.get('QN_2', None)
        if soil_temperature.qn_2 == '-999':
            soil_temperature.qn_2 = None

        soil_temperature.v_te002m = item.get('V_TE002', None)
        if soil_temperature.v_te002m == '-999':
            soil_temperature.v_te002m = None

        soil_temperature.v_te005m = item.get('V_TE005', None)
        if soil_temperature.v_te005m == '-999':
            soil_temperature.v_te005m = None

        soil_temperature.v_te010m = item.get('V_TE010', None)
        if soil_temperature.v_te010m == '-999':
            soil_temperature.v_te010m = None

        soil_temperature.v_te020m = item.get('V_TE020', None)
        if soil_temperature.v_te020m == '-999':
            soil_temperature.v_te020m = None

        soil_temperature.v_te050m = item.get('V_TE050', None)
        if soil_temperature.v_te050m == '-999':
            soil_temperature.v_te050m = None

        soil_temperature.v_te100m = item.get('V_TE100', None)
        if soil_temperature.v_te100m == '-999':
            soil_temperature.v_te100m = None

        return soil_temperature

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_2,
                item.v_te002m,
                item.v_te005m,
                item.v_te010m,
                item.v_te020m,
                item.v_te050m,
                item.v_te100m)

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