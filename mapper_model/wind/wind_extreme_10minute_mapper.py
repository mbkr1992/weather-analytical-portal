from mapper_model.wind.wind_mapper import WindMapper
from model.wind import Wind
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class WindExtreme10MinuteMapper(WindMapper):

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

        wind.fx_10 = item.get('FX_10', None)
        if wind.fx_10 == '-999':
            wind.fx_10 = None

        wind.fnx_10 = item.get('FNX_10', None)
        if wind.fnx_10 == '-999':
            wind.fnx_10 = None

        wind.fmx_10 = item.get('FMX_10', None)
        if wind.fmx_10 == '-999':
            wind.fmx_10 = None

        wind.dx_10 = item.get('DX_10', None)
        if wind.dx_10 == '-999':
            wind.dx_10 = None

        return wind

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
