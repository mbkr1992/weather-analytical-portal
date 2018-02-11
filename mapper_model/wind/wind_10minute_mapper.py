from mapper_model.mapper import Mapper
from model.wind import Wind
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class Wind10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        wind.measurement_category = '10_minutes'

        wind.information = list()

        qn = item.get('QN', None)
        # if self.is_valid(qn):
        #     wind.information.append(
        #         dict(
        #             name='QN',
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        ff_10 = item.get('FF_10', None)
        if self.is_valid(ff_10):
            wind.information.append(
                dict(
                    name='FF_10',
                    value=ff_10,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        dd_10 = item.get('DD_10', None)
        if self.is_valid(dd_10):
            wind.information.append(
                dict(
                    name='DD_10',
                    value=dd_10,
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
