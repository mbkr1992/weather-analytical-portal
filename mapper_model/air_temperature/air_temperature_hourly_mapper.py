from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class AirTemperatureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'air_temperature_qn_9, air_temperature_tt_tu, air_temperature_rf_tu) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING'

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        air_temperature.measurement_category = 'hourly'

        air_temperature.qn_9 = item.get('QN_9', None)
        if air_temperature.qn_9 == '-999':
            air_temperature.qn_9 = None

        air_temperature.tt_tu = item.get('TT_TU', None)
        if air_temperature.tt_tu == '-999':
            air_temperature.tt_tu = None

        air_temperature.rf_tu = item.get('RF_TU', None)
        if air_temperature.rf_tu == '-999':
            air_temperature.rf_tu = None

        return air_temperature

    @staticmethod
    def to_tuple(item: AirTemperature):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_9,
                item.tt_tu,
                item.rf_tu)

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
