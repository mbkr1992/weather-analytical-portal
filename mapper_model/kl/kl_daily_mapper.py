from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class KlDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = 'INSERT INTO data_hub (' \
                             'station_id, measurement_date, measurement_category, ' \
                             'kl_qn_3, kl_fx, kl_fm, ' \
                             'kl_qn_4, kl_rsk, kl_rskf, ' \
                             'kl_sdk, kl_shk_tag, kl_nm, kl_vpm, kl_pm, ' \
                             'kl_tmk, kl_upm, kl_txk, kl_tnk, kl_tgk) ' \
                             'VALUES %s' \
                             'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%s) WHERE path =(%s);'

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

    @staticmethod
    def to_tuple(item: Kl):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_3,
                item.fx,
                item.fm,
                item.qn_4,
                item.rsk,
                item.rskf,
                item.sdk,
                item.shk_tag,
                item.nm,
                item.vpm,
                item.pm,
                item.tmk,
                item.upm,
                item.txk,
                item.tnk,
                item.tgk)

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

