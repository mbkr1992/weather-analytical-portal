from mapper_model.mapper import Mapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class SoilTemperatureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        soil_temperature = SoilTemperature()
        soil_temperature.station_id = item['STATIONS_ID']
        soil_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        soil_temperature.measurement_category = 'hourly'

        soil_temperature.information = list()

        # qn_2 = item.get('QN_2', None)
        # if self.is_valid(qn_2):
        #     soil_temperature.information.append(
        #         dict(
        #             value=qn_2,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        v_te002m = item.get('V_TE002', None)
        if self.is_valid(v_te002m):
            soil_temperature.information.append(
                dict(
                    name='V_TE002',
                    value=v_te002m,
                    unit='°C',
                    description='soil temperature in 2 cm depth',
                )
            )

        v_te005m = item.get('V_TE005', None)
        if self.is_valid(v_te005m):
            soil_temperature.information.append(
                dict(
                    name='V_TE005',
                    value=v_te005m,
                    unit='°C',
                    description='soil temperature in 5 cm depth',
                )
            )

        v_te010m = item.get('V_TE010', None)
        if self.is_valid(v_te010m):
            soil_temperature.information.append(
                dict(
                    name='V_TE010',
                    value=v_te010m,
                    unit='°C',
                    description='soil temperature in 10 cm depth',
                )
            )

        v_te020m = item.get('V_TE020', None)
        if self.is_valid(v_te020m):
            soil_temperature.information.append(
                dict(
                    name='V_TE020',
                    value=v_te020m,
                    unit='°C',
                    description='soil temperature in 20 cm depth',
                )
            )

        v_te050m = item.get('V_TE050', None)
        if self.is_valid(v_te050m):
            soil_temperature.information.append(
                dict(
                    name='V_TE050',
                    value=v_te050m,
                    unit='°C',
                    description='soil temperature in 50 cm depth',
                )
            )

        v_te100 = item.get('V_TE100', None)
        if self.is_valid(v_te100):
            soil_temperature.information.append(
                dict(
                    name='V_TE100',
                    value=v_te100,
                    unit='°C',
                    description='soil temperature in 100 cm depth',
                )
            )

        return soil_temperature

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