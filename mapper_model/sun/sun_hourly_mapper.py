from mapper_model.sun.sun_mapper import SunMapper
from model.sun import Sun
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class SunHourlyMapper(SunMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        sun = Sun()
        sun.station_id = item['STATIONS_ID']
        sun.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        sun.measurement_category = 'hourly'

        sun.qn_7 = item.get('QN_7', None)
        if sun.qn_7 == '-999':
            sun.qn_7 = None

        sun.sd_so = item.get('SD_SO', None)
        if sun.sd_so == '-999':
            sun.sd_so = None

        return sun

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
