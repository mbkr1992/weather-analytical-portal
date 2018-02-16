from mapper_model.mapper import Mapper
from model.water_equiv import WaterEquiv
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class WaterEquivDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        water_equiv = WaterEquiv()
        water_equiv.station_id = item['STATIONS_ID']
        water_equiv.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        water_equiv.measurement_category = 'daily'

        water_equiv.information = list()

        # qn_6 = item.get('QN_6', None)
        # if self.is_valid(qn_6):
        #     water_equiv.information.append(
        #         dict(
        #             value=qn_6,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        ash_6 = item.get('ASH_6', None)
        if self.is_valid(ash_6):
            water_equiv.information.append(
                dict(
                    name='ASH_6',
                    value=ash_6,
                    unit='cm',
                    description='height of snow pack sample',
                )
            )

        sh_tag = item.get('SH_tag', None)
        if self.is_valid(sh_tag):
            water_equiv.information.append(
                dict(
                    name='SH_tag',
                    value=sh_tag,
                    unit='cm',
                    description='total snow depth',
                )
            )

        wash_6 = item.get('WASH_6', None)
        if self.is_valid(wash_6):
            water_equiv.information.append(
                dict(
                    name='WASH_6',
                    value=wash_6,
                    unit='nm',
                    description='total snow water equivalent',
                )
            )

        waas_6 = item.get('WAAS_6', None)
        if self.is_valid(waas_6):
            water_equiv.information.append(
                dict(
                    name='WAAS_6',
                    value=waas_6,
                    unit='nm',
                    description='sampled snow pack water eqivalent',
                )
            )

        return water_equiv

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
