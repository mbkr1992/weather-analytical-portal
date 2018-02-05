from mapper_model.pressure.pressure_mapper import PressureMapper
from model.pressure import Pressure
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class PressureHourlyMapper(PressureMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        pressure = Pressure()
        pressure.station_id = item['STATIONS_ID']
        pressure.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        pressure.measurement_category = 'hourly'

        pressure.qn_8 = item.get('QN_8', None)
        if pressure.qn_8 == '-999':
            pressure.qn_8 = None

        pressure.p = item.get('P', None)
        if pressure.p == '-999':
            pressure.p = None

        pressure.p0 = item.get('P0', None)
        if pressure.p0 == '-999':
            pressure.p0 = None

        return pressure

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
