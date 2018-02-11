from mapper_model.mapper import Mapper
from model.more_precip import MorePrecip
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class MorePrecipDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.select_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        more_precip = MorePrecip()
        more_precip.station_id = item['STATIONS_ID']
        more_precip.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        more_precip.measurement_category = 'daily'

        more_precip.information = list()

        # qn_6 = item.get('QN_6', None)
        # if self.is_valid(qn_6):
        #     more_precip.information.append(
        #         dict(
        #             value=qn_6,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        rs = item.get('RS', None)
        if self.is_valid(rs):
            more_precip.information.append(
                dict(
                    name='RS',
                    value=rs,
                    unit='mm',
                    description='daily precipitation height',
                )
            )

        rsf = item.get('RSF', None)
        if self.is_valid(rsf):
            more_precip.information.append(
                dict(
                    name='RSF',
                    value=rsf,
                    unit=NOT_AVAILABLE,
                    description='precipitation form',
                )
            )

        sh_tag = item.get('SH_TAG', None)
        if self.is_valid(sh_tag):
            more_precip.information.append(
                dict(
                    name='SH_TAG',
                    value=sh_tag,
                    unit='cm',
                    description='daily height of snow pack',
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
