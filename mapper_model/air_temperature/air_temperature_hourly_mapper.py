from mapper_model.air_temperature.air_temperature_mapper import AirTemperatureMapper
from model.air_temperature import AirTemperature
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class AirTemperatureHourlyMapper(AirTemperatureMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        air_temperature.measurement_category = 'hourly'

        air_temperature.qn_9 = item.get('QN_9', None)
        if air_temperature.qn_9 == '-999':
            air_temperature.qn_9 = None

        air_temperature.tt_tu = item.get('TT_TU', None)
        if air_temperature.tt_tu == '-999':
            air_temperature.tt_tu = None

        air_temperature.rf_tu = item.get('RF_TU', None)
        if air_temperature.rf_tu == '-999':
            air_temperature.rf_tu = None

        return air_temperature

    def insert_items(self, items):
        super(AirTemperatureHourlyMapper, self).insert_items(items)

    def update_file_parsed_flag(self, path):
        super(AirTemperatureHourlyMapper, self).update_file_parsed_flag(path)
