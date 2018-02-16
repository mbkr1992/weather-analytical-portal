from mapper_model.mapper import Mapper
from model.sun import Sun
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class SunHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        sun = Sun()
        sun.station_id = item['STATIONS_ID']
        sun.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        sun.measurement_category = 'hourly'

        sun.information = list()
        # qn_7 = item.get('QN_7', None)
        # if self.is_valid(qn_7):
        #     sun.information.append(
        #         dict(
        #             value=qn_7,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        sd_so = item.get('SD_SO', None)
        if self.is_valid(sd_so):
            sun.information.append(
                dict(
                    name='SD_SO',
                    value=sd_so,
                    unit='min',
                    description='hourly sunshine duration',
                )
            )

        return sun

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

