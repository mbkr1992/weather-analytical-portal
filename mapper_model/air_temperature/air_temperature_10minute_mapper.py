from mapper_model.air_temperature.air_temperature_mapper import AirTemperatureMapper
from model.air_temperature import AirTemperature
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class AirTemperature10MinuteMapper(AirTemperatureMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        air_temperature = AirTemperature()
        air_temperature.station_id = item['STATIONS_ID']
        air_temperature.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H%M')
        air_temperature.measurement_category = '10_minute'

        air_temperature.qn = item.get('QN', None)
        if air_temperature.qn == '-999':
            air_temperature.qn = None

        air_temperature.pp_10 = item.get('PP_10', None)
        if air_temperature.pp_10 == '-999':
            air_temperature.pp_10 = None

        air_temperature.tt_10 = item.get('TT_10', None)
        if air_temperature.tt_10 == '-999':
            air_temperature.tt_10 = None

        air_temperature.tm5_10 = item.get('TM5_10', None)
        if air_temperature.tm5_10 == '-999':
            air_temperature.tm5_10 = None

        air_temperature.rf_10 = item.get('RF_10', None)
        if air_temperature.rf_10 == '-999':
            air_temperature.rf_10 = None

        air_temperature.td_10 = item.get('TD_10', None)
        if air_temperature.td_10 == '-999':
            air_temperature.td_10 = None

        return air_temperature

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
