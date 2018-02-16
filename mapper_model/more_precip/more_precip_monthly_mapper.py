from mapper_model.mapper import Mapper
from model.more_precip import MorePrecip
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class MorePrecipMonthlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        more_precip = MorePrecip()
        more_precip.station_id = item['STATIONS_ID']
        more_precip.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d')
        more_precip.measurement_category = 'daily'

        more_precip.information = list()

        # qn_6 = item.get('QN_6', None)
        # if self.is_valid(qn_6):
        #     more_precip.information.append(
        #         dict(
        #             name='QN_6',
        #             value=qn_6,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        mo_nsh = item.get('MO_NSH', None)
        if self.is_valid(mo_nsh):
            more_precip.information.append(
                dict(
                    name='MO_NSH',
                    value=mo_nsh,
                    unit='cm',
                    description='monthly sum of daily fresh snow',
                )
            )

        mo_rr = item.get('MO_RR', None)
        if self.is_valid(mo_rr):
            more_precip.information.append(
                dict(
                    name='MO_RR',
                    value=mo_rr,
                    unit='mm',
                    description='monthly sum of daily precipitation height',
                )
            )

        mo_sh_s = item.get('MO_SH_S', None)
        if self.is_valid(mo_sh_s):
            more_precip.information.append(
                dict(
                    name='MO_SH_S',
                    value=mo_sh_s,
                    unit='cm',
                    description='monthly sum of daily height of snow pack',
                )
            )

        max_rs = item.get('MX_RS', None)
        if self.is_valid(max_rs):
            more_precip.information.append(
                dict(
                    name='MX_RS',
                    value=max_rs,
                    unit='mm',
                    description='monthly max of daily precipitation height',
                )
            )

        return more_precip


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
                extras.execute_values(curs, self.select_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)
