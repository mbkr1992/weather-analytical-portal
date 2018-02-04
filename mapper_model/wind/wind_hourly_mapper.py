from mapper_model.wind.wind_mapper import WindMapper
from model.wind import Wind
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class WindHourlyMapper(WindMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        wind.measurement_category = 'hourly'

        wind.qn_3 = item.get('QN_3', None)
        if wind.qn_3 == '-999':
            wind.qn_3 = None

        wind.f = item.get('F', None)
        if wind.f == '-999':
            wind.f = None

        wind.d = item.get('D', None)
        if wind.d == '-999':
            wind.d = None

        return wind

    def insert_items(self, items):
        super(WindHourlyMapper, self).insert_items(items)

    def update_file_parsed_flag(self, path):
        super(WindHourlyMapper, self).update_file_parsed_flag(path)
