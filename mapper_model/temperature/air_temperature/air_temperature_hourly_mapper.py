from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class AirTemperatureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        air_temperature.measurement_category = 'hourly'

        air_temperature.information = list()

        # qn_9 = item.get('QN_9', None)
        # if self.is_valid(qn_9):
        #     air_temperature.information.append(
        #         dict(
        #             value=qn_9,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        tt_tu = item.get('TT_TU', None)
        if self.is_valid(tt_tu):
            air_temperature.information.append(
                dict(
                    name='TT_TU',
                    value=tt_tu,
                    unit='°C',
                    description='2m air temperature',
                )
            )

        rf_tu = item.get('RF_TU', None)
        if self.is_valid(rf_tu):
            air_temperature.information.append(
                dict(
                    name='RF_TU',
                    value=rf_tu,
                    unit='%',
                    description='2m relative humidity',
                )
            )

        return air_temperature

    @staticmethod
    def is_valid(value):
        return value and value != '999'

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                extras.Json(item.information))

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
