from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class AirTemperature10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'air_temperature_qn, air_temperature_pp_10, air_temperature_tt_10, ' \
                            'air_temperature_tm5_10, air_temperature_rf_10, air_temperature_td_10) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING'

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        air_temperature.measurement_category = '10_minute'

        air_temperature.qn = item.get('QN', None)
        if air_temperature.qn == '-999':
            air_temperature.qn = None

        air_temperature.pp_10 = item.get('PP_10', None)
        if air_temperature.pp_10 == '-999':
            air_temperature.pp_10 = None

        air_temperature.tt_10 = item.get('TT_10', None)
        if air_temperature.tt_10 == '-999':
            air_temperature.tt_10 = None

        air_temperature.tm5_10 = item.get('TM5_10', None)
        if air_temperature.tm5_10 == '-999':
            air_temperature.tm5_10 = None

        air_temperature.rf_10 = item.get('RF_10', None)
        if air_temperature.rf_10 == '-999':
            air_temperature.rf_10 = None

        air_temperature.td_10 = item.get('TD_10', None)
        if air_temperature.td_10 == '-999':
            air_temperature.td_10 = None

        return air_temperature

    @staticmethod
    def to_tuple(item: AirTemperature):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.pp_10,
                item.tt_10,
                item.tm5_10,
                item.rf_10,
                item.td_10)

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
