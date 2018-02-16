from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class ExtremeTemperature10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        air_temperature.measurement_category = '10_minutes'

        air_temperature.information = list()

        # qn = item.get('QN', None)
        # if self.is_valid(qn):
        #     air_temperature.information.append(
        #         dict(
        #             name='QN',
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        tx = item.get('TX_10', None)
        if self.is_valid(tx):
            air_temperature.information.append(
                dict(
                    name='TX_10',
                    value=tx,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        # txt file has same two columns
        # air_temperature.tx5 = item.get('TX_10', None)
        # if air_temperature.tx5 == '-999':
        #     air_temperature.tx5 = None

        tn = item.get('TN_10', None)
        if self.is_valid(tn):
            air_temperature.information.append(
                dict(
                    name='TN_10',
                    value=tn,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        tn5 = item.get('TN5_10', None)
        if self.is_valid(tn5):
            air_temperature.information.append(
                dict(
                    name='TN5_10',
                    value=tn5,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
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
