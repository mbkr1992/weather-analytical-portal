from mapper_model.mapper import Mapper
from model.precipitation import Precipitation
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class Precipitation1MinuteMapper(Mapper):
    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (station_id, measurement_date, measurement_category, information)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d%H%M')
        precipitation.measurement_category = '1_minute'

        precipitation.information = list()

        # qn = item.get('QN', None)
        # if self.is_valid(qn):
        #     precipitation.information.append(
        #         dict(
        #             name='QN',
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description=NOT_AVAILABLE,
        #         )
        #     )

        rs_01 = item.get('RS_01', None)
        if self.is_valid(rs_01):
            precipitation.information.append(
                dict(
                    name='RS_01',
                    value=rs_01,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        rth_01 = item.get('RTH_01', None)
        if self.is_valid(rth_01):
            precipitation.information.append(
                dict(
                    name='RTH_01',
                    value=rth_01,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        rwh_01 = item.get('RWH_01', None)
        if self.is_valid(rwh_01):
            precipitation.information.append(
                dict(
                    name='RWH_01',
                    value=rwh_01,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
                )
            )

        rs_ind_01 = item.get('RS_IND_01', None)
        if self.is_valid(rs_ind_01):
            precipitation.information.append(
                dict(
                    name='RS_IND_01',
                    value=rs_ind_01,
                    unit=NOT_AVAILABLE,
                    description=NOT_AVAILABLE,
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