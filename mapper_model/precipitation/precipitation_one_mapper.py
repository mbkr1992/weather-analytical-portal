from mapper_model.precipitation.precipitation_mapper import PrecipitationMapper
from model.precipitation import Precipitation
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class PrecipitationOneMapper(PrecipitationMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d%H%M')
        precipitation.measurement_category = '1_minute'
        precipitation.qn = item.get('QN', None)

        precipitation.rs_01 = item.get('RS_01', None)
        if precipitation.rs_01 == '-999':
            precipitation.rs_01 = None

        precipitation.rth_01 = item.get('RTH_01', None)
        if precipitation.rth_01 == '-999':
            precipitation.rth_01 = None

        precipitation.rwh_01 = item.get('RWH_01', None)
        if precipitation.rwh_01 == '-999':
            precipitation.rwh_01 = None

        precipitation.rs_ind_01 = item.get('RS_IND_01', None)
        if precipitation.rs_ind_01 == '-999':
            precipitation.rs_ind_01 = None

        return precipitation

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
