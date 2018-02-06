from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class KlMonthlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'kl_qn_4, kl_mo_n, kl_mo_tt, ' \
                            'kl_mo_tx, kl_mo_tn, kl_mo_fk, ' \
                            'kl_mx_tx, kl_mx_fx, kl_mx_tn, kl_mo_sd_s, qn_6, ' \
                            'kl_mo_rr, kl_mxrs) ' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

    def map(self, item={}):
        kl = Kl()
        kl.station_id = item['STATIONS_ID']
        kl.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d')
        kl.measurement_category = 'monthly'

        kl.qn_4 = item.get('QN_4', None)
        if kl.qn_4 == '-999':
            kl.qn_4 = None

        kl.mo_n = item.get('MO_N', None)
        if kl.mo_n == '-999':
            kl.mo_n = None

        kl.mo_tt = item.get('MO_TT', None)
        if kl.mo_tt == '-999':
            kl.mo_tt = None

        kl.mo_tx = item.get('MO_TX', None)
        if kl.mo_tx == '-999':
            kl.mo_tx = None

        kl.mo_tn = item.get('MO_TN', None)
        if kl.mo_tn == '-999':
            kl.mo_tn = None

        kl.mo_fk = item.get('MO_FK', None)
        if kl.mo_fk == '-999':
            kl.mo_fk = None

        kl.mx_tx = item.get('MX_TX', None)
        if kl.mx_tx == '-999':
            kl.mx_tx = None

        kl.mx_fx = item.get('MX_FX', None)
        if kl.mx_fx == '-999':
            kl.mx_fx = None

        kl.mx_tn = item.get('MX_TN', None)
        if kl.mx_tn == '-999':
            kl.mx_tn = None

        kl.mo_sd_s = item.get('MO_SD_S', None)
        if kl.mo_sd_s == '-999':
            kl.mo_sd_s = None

        kl.qn_6 = item.get('QN_6', None)
        if kl.qn_6 == '-999':
            kl.qn_6 = None

        kl.mo_rr = item.get('MO_RR', None)
        if kl.mo_rr == '-999':
            kl.mo_rr = None

        kl.mx_rs = item.get('MX_RS', None)
        if kl.mx_rs == '-999':
            kl.mx_rs = None

        return kl

    @staticmethod
    def to_tuple(item: Kl):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_4,
                item.mo_n,
                item.mo_tt,
                item.mo_tx,
                item.mo_tn,
                item.mo_fk,
                item.mx_tx,
                item.mx_fx,
                item.mx_tn,
                item.mo_sd_s,
                item.qn_6,
                item.mo_rr,
                item.mx_rs)

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
                curs.execute(self.update_file, data)
