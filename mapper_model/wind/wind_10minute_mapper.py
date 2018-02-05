from mapper_model.wind.wind_mapper import WindMapper
from model.wind import Wind
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class Wind10MinuteMapper(WindMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        wind.measurement_category = '10_minute'

        wind.qn = item.get('QN', None)
        if wind.qn == '-999':
            wind.qn = None

        wind.ff_10 = item.get('FF_10', None)
        if wind.ff_10 == '-999':
            wind.ff_10 = None

        wind.dd_10 = item.get('DD_10', None)
        if wind.dd_10 == '-999':
            wind.dd_10 = None

        return wind

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
