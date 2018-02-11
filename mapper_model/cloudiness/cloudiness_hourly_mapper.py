from mapper_model.mapper import Mapper
from model.cloudiness import Cloudiness
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class CloudinessHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        cloudiness = Cloudiness()
        cloudiness.station_id = item['STATIONS_ID']
        cloudiness.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        cloudiness.measurement_category = 'hourly'

        cloudiness.information = list()

        # qn_8 = item.get('QN_8', None)
        # if self.is_valid(qn_8):
        #     cloudiness.information.append(
        #         dict(
        #             name='QN_8',
        #             value=qn_8,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        v_n_i = item.get('V_N_I', None)
        if self.is_valid(v_n_i):
            cloudiness.information.append(
                dict(
                    name='V_N_I',
                    value=v_n_i,
                    unit=NOT_AVAILABLE,
                    description='index how measurement is taken '
                                'P = by human person '
                                'I = by instrument ',
                )
            )

        v_n = item.get('V_N', None)
        if self.is_valid(v_n):
            cloudiness.information.append(
                dict(
                    name='V_N',
                    value=v_n,
                    unit=NOT_AVAILABLE,
                    description='1/8 = total cloud cover '
                                '-1 = not determined ',
                )
            )

        return cloudiness

    @staticmethod
    def is_valid(value):
        return value and value != '999'

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                extras.Json(item.information),
                )

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
