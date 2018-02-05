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
        precipitation.measurement_category = '10_minute'
        precipitation.qn = item.get('QN', None)

        precipitation.rws_dau_10 = item.get('RWS_DAU_10', None)
        if precipitation.rws_dau_10 == '-999':
            precipitation.rws_dau_10 = None

        precipitation.rws_10 = item.get('RWS_10', None)
        if precipitation.rws_10 == '-999':
            precipitation.rws_10 = None

        precipitation.rws_ind_10 = item.get('RWS_IND_10', None)
        if precipitation.rws_ind_10 == '-999':
            precipitation.rws_ind_10 = None

        return precipitation

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
