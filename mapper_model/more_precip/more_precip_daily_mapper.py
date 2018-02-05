from mapper_model.more_precip.more_precip_mapper import MorePrecipMapper
from model.more_precip import MorePrecip
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class MorePrecipDailyMapper(MorePrecipMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        more_precip = MorePrecip()
        more_precip.station_id = item['STATIONS_ID']
        more_precip.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        more_precip.measurement_category = 'daily'

        more_precip.qn_6 = item.get('QN_6', None)
        if more_precip.qn_6 == '-999':
            more_precip.qn_6 = None

        more_precip.rs = item.get('RS', None)
        if more_precip.rs == '-999':
            more_precip.rs = None

        more_precip.rsf = item.get('RSF', None)
        if more_precip.rsf == '-999':
            more_precip.rsf = None

        more_precip.sh_tag = item.get('SH_TAG', None)
        if more_precip.sh_tag == '-999':
            more_precip.sh_tag = None

        return more_precip

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
