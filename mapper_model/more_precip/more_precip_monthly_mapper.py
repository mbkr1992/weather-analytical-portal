from mapper_model.mapper import Mapper
from model.more_precip import MorePrecip
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class MorePrecipMonthlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.select_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'more_precip_qn_6, more_precip_mo_nsh, more_precip_mo_rr, more_precip_mo_sh_s, more_precip_mx_rs) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING'

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        more_precip = MorePrecip()
        more_precip.station_id = item['STATIONS_ID']
        more_precip.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d')
        more_precip.measurement_category = 'daily'

        more_precip.qn_6 = item.get('QN_6', None)
        if more_precip.qn_6 == '-999':
            more_precip.qn_6 = None

        more_precip.mo_nsh = item.get('MO_NSH', None)
        if more_precip.mo_nsh == '-999':
            more_precip.mo_nsh = None

        more_precip.mo_rr = item.get('MO_RR', None)
        if more_precip.mo_rr == '-999':
            more_precip.mo_rr = None

        more_precip.mo_sh_s = item.get('MO_SH_S', None)
        if more_precip.mo_sh_s == '-999':
            more_precip.mo_sh_s = None

        more_precip.max_rs = item.get('MX_RS', None)
        if more_precip.max_rs == '-999':
            more_precip.max_rs = None

        return more_precip

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_6,
                item.mo_nsh,
                item.mo_rr,
                item.mo_sh_s,
                item.max_rs)

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
