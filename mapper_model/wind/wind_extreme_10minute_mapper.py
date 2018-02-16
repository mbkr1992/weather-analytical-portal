from mapper_model.mapper import Mapper
from model.wind import Wind
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class WindExtreme10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        wind.measurement_category = '10_minutes'

        # qn = item.get('QN', None)
        # if self.is_valid(qn):
        #     wind.information.append(
        #         dict(
        #             name='QN',
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        fx_10 = item.get('FX_10', None)
        if self.is_valid(fx_10):
            wind.information.append(
                dict(
                    name='FX_10',
                    value=fx_10,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        fnx_10 = item.get('FNX_10', None)
        if self.is_valid(fnx_10):
            wind.information.append(
                dict(
                    name='FNX_10',
                    value=fnx_10,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        fmx_10 = item.get('FMX_10', None)
        if self.is_valid(fmx_10):
            wind.information.append(
                dict(
                    name='FMX_10',
                    value=fmx_10,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        dx_10 = item.get('DX_10', None)
        if self.is_valid(dx_10):
            wind.information.append(
                dict(
                    name='DX_10',
                    value=dx_10,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        return wind

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
