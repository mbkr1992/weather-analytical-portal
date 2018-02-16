from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class PrecipitationHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        precipitation.measurement_category = 'hourly'

        precipitation.information = list()

        # qn_8 = item.get('QN_8', None)
        # if self.is_valid(qn_8):
        #     precipitation.information.append(
        #         dict(
        #             value=qn_8,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        r1 = item.get('R1', None)
        if self.is_valid(r1):
            precipitation.information.append(
                dict(
                    name='R1',
                    value=r1,
                    unit='mm',
                    description='hourly precipitation height',
                )
            )

        wrtr = item.get('WRTR', None)
        if self.is_valid(wrtr):
            precipitation.information.append(
                dict(
                    name='WRTR',
                    value=wrtr,
                    unit='WR-code',
                    description='form of precipitation',
                )
            )

        rs_ind = item.get('RS_IND', None)
        if self.is_valid(rs_ind):
            precipitation.information.append(
                dict(
                    name='RS_IND',
                    value=rs_ind,
                    unit=NOT_AVAILABLE,
                    description='0 = no precipitation '
                                '1 = precipitation has fallen ',
                )
            )

        return precipitation

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
