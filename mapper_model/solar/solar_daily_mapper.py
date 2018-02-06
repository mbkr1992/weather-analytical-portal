from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class SolarDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT ' \
                            'INTO data_hub (station_id, measurement_date, measurement_category, solar_qn, solar_atmo, solar_fd, ' \
                            'solar_fg, solar_sd) ' \
                            'VALUES %s ' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):

        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        solar.measurement_category = 'hourly'
        solar.qn = item.get('QN_592', None)

        solar.atmo_radiation = item.get('ATMO_STRAHL', None)
        if solar.atmo_radiation == '-999':
            solar.atmo_radiation = None

        solar.fd_radiation = item.get('FD_STRAHL', None)
        if solar.fd_radiation == '-999':
            solar.fd_radiation = None

        solar.fg_radiation = item.get('FG_STRAHL', None)
        if solar.fg_radiation == '-999':
            solar.fg_radiation = None

        solar.sd_radiation = item.get('SD_STRAHL', None)
        if solar.sd_radiation == '-999':
            solar.sd_radiation = None

        return solar

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.atmo_radiation,
                item.fd_radiation,
                item.fg_radiation,
                item.sd_radiation)

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
