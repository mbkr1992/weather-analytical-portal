from mapper_model.mapper import Mapper
from model.visibility import Visibility
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class VisibilityHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        visibility = Visibility()
        visibility.station_id = item['STATIONS_ID']
        visibility.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        visibility.measurement_category = 'hourly'
        visibility.information = list()
        # qn_8 = item.get('QN_8', None)
        # if self.is_valid(qn_8):
        #     wind.information.append(
        #         dict(
        #             value=qn_8,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        v_vv_i = item.get('V_VV_I', None)
        if self.is_valid(v_vv_i):
            visibility.information.append(
                dict(
                    name='V_VV_I',
                    value=v_vv_i,
                    unit=NOT_AVAILABLE,
                    description='index how measurement is taken '
                                'P=human'
                                'I=instrument',
                )
            )

        v_vv = item.get('V_VV', None)
        if self.is_valid(v_vv):
            visibility.information.append(
                dict(
                    name='V_VV',
                    value=v_vv,
                    unit='m',
                    description='visibility',
                )
            )

        return visibility

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

