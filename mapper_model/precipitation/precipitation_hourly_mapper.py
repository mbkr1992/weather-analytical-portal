from mapper_model.precipitation.precipitation_mapper import PrecipitationMapper
from model.precipitation import Precipitation
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class PrecipitationHourlyMapper(PrecipitationMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        precipitation = Precipitation()
        precipitation.station_id = item['STATIONS_ID']
        precipitation.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        precipitation.measurement_category = 'hourly'
        precipitation.qn_8 = item.get('QN_8', None)

        precipitation.r1 = item.get('R1', None)
        if precipitation.r1 == '-999':
            precipitation.r1 = None

        precipitation.wrtr = item.get('WRTR', None)
        if precipitation.wrtr == '-999':
            precipitation.wrtr = None

        precipitation.rs_ind = item.get('RS_IND', None)
        if precipitation.rs_ind == '-999':
            precipitation.rs_ind = None

        return precipitation

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
