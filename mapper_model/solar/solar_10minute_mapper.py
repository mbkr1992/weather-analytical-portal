from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class Solar10MinuteMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT ' \
                            'INTO data_hub (station_id, measurement_date, measurement_category, solar_qn, solar_ds, ' \
                            'solar_gs, solar_sd, solar_ls) ' \
                            'VALUES %s ' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):

        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        solar.measurement_category = '10_minutes'
        solar.qn = item.get('QN', None)

        solar.ds = item.get('DS_10', None)
        if solar.ds == '-999':
            solar.ds = None

        solar.gs = item.get('GS_10', None)
        if solar.gs == '-999':
            solar.gs = None

        solar.sd = item.get('SD_10', None)
        if solar.sd == '-999':
            solar.sd = None

        solar.ls = item.get('LS_10', None)
        if solar.ls == '-999':
            solar.ls = None

        return solar

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn,
                item.ds,
                item.gs,
                item.sd,
                item.ls)

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
