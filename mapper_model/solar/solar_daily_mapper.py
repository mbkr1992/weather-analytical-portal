from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE


class SolarDailyMapper(Mapper):

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
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        solar.measurement_category = 'hourly'

        solar.information = list()

        # qn = item.get('QN_592', None)
        # if self.is_valid(qn):
        #     solar.information.append(
        #         dict(
        #             name='QN_592',
        #             value=qn,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        atmo_radiation = item.get('ATMO_STRAHL', None)
        if self.is_valid(atmo_radiation):
            solar.information.append(
                dict(
                    name='ATMO_STRAHL',
                    value=atmo_radiation,
                    unit='J/cm^2',
                    description='longwave downward radiation',
                )
            )

        fd_radiation = item.get('FD_STRAHL', None)
        if self.is_valid(fd_radiation):
            solar.information.append(
                dict(
                    name='FD_STRAHL',
                    value=fd_radiation,
                    unit='J/cm^2',
                    description='daily sum of diffuse solar radiation',
                )
            )

        fg_radiation = item.get('FG_STRAHL', None)
        if self.is_valid(fg_radiation):
            solar.information.append(
                dict(
                    name='FG_STRAHL',
                    value=fg_radiation,
                    unit='J/cm^2',
                    description='daily sum of solar incoming radiation',
                )
            )

        sd_radiation = item.get('SD_STRAHL', None)
        if self.is_valid(sd_radiation):
            solar.information.append(
                dict(
                    name='SD_STRAHL',
                    value=sd_radiation,
                    unit='min',
                    description='daily sum of sunshine duration',
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
