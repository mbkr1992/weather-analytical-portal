from constants import constants
from parser_model.file_soil_station_parser import FileSoilStationParser
from parser_model.simple_parser import SimpleParser
from parser_model.empty_parser import EmptyParser
from parser_model.station_parser import StationParser
from common.helper import Helper


class ParserFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_parser(identifier):
        if identifier == constants.PARSER_STATION:
            return FileSoilStationParser()
        elif identifier == constants.MAPPER_STATION:
            return StationParser()
        elif identifier == constants.PARSER_SIMPLE:
            return SimpleParser()
        return EmptyParser()

    @staticmethod
    def get_parser_for_path(path):
        if Helper.is_solar_daily_file(path)\
                or Helper.is_solar_10minute_file(path)\
                or Helper.is_solar_hourly_file(path)\
                or Helper.is_precipitation_one_file(path)\
                or Helper.is_precipitation_hourly_file(path)\
                or Helper.is_kl_daily_file(path)\
                or Helper.is_more_precip_daily_file(path)\
                or Helper.is_soil_temperature_daily(path)\
                or Helper.is_water_equiv_daily(path)\
                or Helper.is_air_temperature_hourly(path)\
                or Helper.is_cloudiness_hourly(path)\
                or Helper.is_wind_hourly(path)\
                or Helper.is_wind_10minute(path)\
                or Helper.is_visibility_hourly(path)\
                or Helper.is_pressure_hourly(path)\
                or Helper.is_sun_hourly(path)\
                or Helper.is_air_temperature_10minute(path)\
                or Helper.is_kl_10minute_file(path):

            return SimpleParser()
        return EmptyParser()