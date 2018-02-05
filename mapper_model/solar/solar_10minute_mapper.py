from mapper_model.solar.solar_mapper import SolarMapper
from model.solar import Solar
from datetime import datetime


class Solar10MinuteMapper(SolarMapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        solar.measurement_category = '10_minute'
        solar.qn = item.get('QN', None)

        solar.ds_10 = item.get('DS_10', None)
        if solar.ds_10 == '-999':
            solar.ds_10 = None

        solar.gs_10 = item.get('GS_10', None)
        if solar.gs_10 == '-999':
            solar.gs_10 = None

        solar.sd_10 = item.get('SD_10', None)
        if solar.sd_10 == '-999':
            solar.sd_10 = None

        solar.ls_10 = item.get('LS_10', None)
        if solar.ls_10 == '-999':
            solar.ls_10 = None

        return solar

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
