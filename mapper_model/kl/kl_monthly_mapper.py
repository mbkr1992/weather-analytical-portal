from mapper_model.kl.kl_mapper import KlMapper
from model.kl import Kl
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class KlMonthlyMapper(KlMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

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

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
