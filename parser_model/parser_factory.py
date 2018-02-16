from constants import constants
from parser_model.simple_parser import SimpleParser
from parser_model.empty_parser import EmptyParser
from parser_model.station_parser import StationParser
from parser_model.grib_parser import GribParser
from common.helper import Helper


class ParserFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_parser(identifier):
        pass

    @staticmethod
    def get_parser_for_path(path):
        if Helper.is_solar_daily_file(path)\
                or Helper.is_solar_10minutes_file(path)\
                or Helper.is_solar_hourly_file(path)\
                or Helper.is_precipitation_1minute_file(path)\
                or Helper.is_precipitation_hourly_file(path)\
                or Helper.is_precipitation_10minutes_file(path)\
                or Helper.is_more_precip_daily_file(path)\
                or Helper.is_more_precip_monthly_file(path)\
                or Helper.is_soil_temperature_daily(path)\
                or Helper.is_soil_temperature_hourly(path)\
                or Helper.is_soil_daily(path)\
                or Helper.is_water_equiv_daily(path)\
                or Helper.is_air_temperature_hourly(path) \
                or Helper.is_air_temperature_10minutes(path) \
                or Helper.is_extreme_temperature_10minutes(path)\
                or Helper.is_cloudiness_hourly(path)\
                or Helper.is_wind_hourly(path)\
                or Helper.is_wind_10minutes(path)\
                or Helper.is_extreme_wind_10minutes(path)\
                or Helper.is_visibility_hourly(path)\
                or Helper.is_pressure_hourly(path)\
                or Helper.is_sun_hourly(path) \
                or Helper.is_kl_daily_file(path) \
                or Helper.is_kl_monthly_file(path):
            return SimpleParser()
        elif Helper.is_station(path):
            return StationParser()
        elif Helper.is_analysis_cams_nrealtime(path)\
            or Helper.is_forecast_cams_nrealtime(path):
            return GribParser()
        return EmptyParser()