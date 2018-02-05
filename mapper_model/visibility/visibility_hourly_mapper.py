from mapper_model.visibility.visibility_mapper import VisibilityMapper
from model.wind import Wind
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class VisibilityHourlyMapper(VisibilityMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        wind = Wind()
        wind.station_id = item['STATIONS_ID']
        wind.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        wind.measurement_category = 'hourly'

        wind.qn_8 = item.get('QN_8', None)
        if wind.qn_8 == '-999':
            wind.qn_8 = None

        wind.v_vv_i = item.get('V_VV_I', None)
        if wind.v_vv_i == '-999':
            wind.v_vv_i = None

        wind.d = item.get('V_VV', None)
        if wind.v_vv == '-999':
            wind.v_vv = None

        return wind

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
