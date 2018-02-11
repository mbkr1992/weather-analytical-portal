from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class Solar10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):

        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        solar.measurement_category = '10_minutes'

        solar.information = list()

        # qn = item.get('QN', None)
        # if self.is_valid(qn):
        #     solar.information.append(
        #         dict(
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description=NOT_AVAILABLE,
        #         )
        #     )

        ds = item.get('DS_10', None)
        if self.is_valid(ds):
            solar.information.append(
                dict(
                    name='DS_10',
                    value=ds,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        gs = item.get('GS_10', None)
        if self.is_valid(gs):
            solar.information.append(
                dict(
                    name='GS_10',
                    value=gs,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        sd = item.get('SD_10', None)
        if self.is_valid(sd):
            solar.information.append(
                dict(
                    name='SD_10',
                    value=sd,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        ls = item.get('LS_10', None)
        if self.is_valid(ls):
            solar.information.append(
                dict(
                    name='LS_10',
                    value=ls,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        return solar

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
