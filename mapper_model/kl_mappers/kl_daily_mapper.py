from mapper_model.kl_mappers.kl_mapper import KlMapper
from model.kl import Kl
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class KlDailyMapper(KlMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        kl = Kl()
        kl.station_id = item['STATIONS_ID']
        kl.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        kl.measurement_category = 'daily'

        kl.qn_3 = item.get('QN_3', None)

        kl.fx = item.get('FX', None)
        if kl.fx == '-999':
            kl.fx = None

        kl.fm = item.get('FM', None)
        if kl.fm == '-999':
            kl.fm = None

        kl.qn_4 = item.get('QN_4', None)
        if kl.qn_4 == '-999':
            kl.qn_4 = None

        kl.rsk = item.get('RSK', None)
        if kl.rsk == '-999':
            kl.rsk = None

        kl.rskf = item.get('RSKF', None)
        if kl.rskf == '-999':
            kl.rskf = None

        kl.sdk = item.get('SDK', None)
        if kl.sdk == '-999':
            kl.sdk = None

        kl.shk_tag = item.get('SHK_TAG', None)
        if kl.shk_tag == '-999':
            kl.shk_tag = None

        kl.nm = item.get('NM', None)
        if kl.nm == '-999':
            kl.nm = None

        kl.vpm = item.get('VPM', None)
        if kl.vpm == '-999':
            kl.vpm = None

        kl.pm = item.get('PM', None)
        if kl.pm == '-999':
            kl.pm = None

        kl.tmk = item.get('TMK', None)
        if kl.tmk == '-999':
            kl.tmk = None

        kl.upm = item.get('UPM', None)
        if kl.upm == '-999':
            kl.upm = None

        kl.txk = item.get('TXK', None)
        if kl.txk == '-999':
            kl.txk = None

        kl.tnk = item.get('TNK', None)
        if kl.tnk == '-999':
            kl.tnk = None

        kl.tgk = item.get('TGK', None)
        if kl.tgk == '-999':
            kl.tgk = None

        return kl

    def insert_items(self, items):
        super(KlDailyMapper, self).insert_items(items)

    def update_file_parsed_flag(self, path):
        super(KlDailyMapper, self).update_file_parsed_flag(path)
