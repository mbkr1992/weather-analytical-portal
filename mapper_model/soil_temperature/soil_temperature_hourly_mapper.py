from mapper_model.soil_temperature.soil_temperature_mapper import SoilTemperatureMapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class SoilTemperatureHourlyMapper(SoilTemperatureMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        soil_temperature = SoilTemperature()
        soil_temperature.station_id = item['STATIONS_ID']
        soil_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        soil_temperature.measurement_category = 'daily'

        soil_temperature.qn_2 = item.get('QN_2', None)
        if soil_temperature.qn_2 == '-999':
            soil_temperature.qn_2 = None

        soil_temperature.v_te002m = item.get('V_TE002', None)
        if soil_temperature.v_te002m == '-999':
            soil_temperature.v_te002m = None

        soil_temperature.v_te005m = item.get('V_TE005', None)
        if soil_temperature.v_te005m == '-999':
            soil_temperature.v_te005m = None

        soil_temperature.v_te010m = item.get('V_TE010', None)
        if soil_temperature.v_te010m == '-999':
            soil_temperature.v_te010m = None

        soil_temperature.v_te020m = item.get('V_TE020', None)
        if soil_temperature.v_te020m == '-999':
            soil_temperature.v_te020m = None

        soil_temperature.v_te050m = item.get('V_TE050', None)
        if soil_temperature.v_te050m == '-999':
            soil_temperature.v_te050m = None

        soil_temperature.v_te100m = item.get('V_TE100', None)
        if soil_temperature.v_te100m == '-999':
            soil_temperature.v_te100m = None

        return soil_temperature

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
