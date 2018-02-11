from mapper_model.mapper import Mapper
from model.pressure import Pressure
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class PressureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        pressure = Pressure()
        pressure.station_id = item['STATIONS_ID']
        pressure.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        pressure.measurement_category = 'hourly'

        pressure.information = list()

        # qn_8 = item.get('QN_8', None)
        # if self.is_valid(qn_8):
        #     pressure.information.append(
        #         dict(
        #             name='QN_8',
        #             value=qn_8,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        p = item.get('P', None)
        if self.is_valid(p):
            pressure.information.append(
                dict(
                    name='P',
                    value=p,
                    unit='hPA',
                    description='mean sea level pressure',
                )
            )

        p0 = item.get('P0', None)
        if self.is_valid(p0):
            pressure.information.append(
                dict(
                    name='P0',
                    value=p0,
                    unit='hPA',
                    description='pressure at station height',
                )
            )

        return pressure

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
