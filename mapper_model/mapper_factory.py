from mapper_model.solar_mappers.solar_mapper import SolarMapper
from common.helper import Helper
from constants.constants import MAPPER_STATION, MAPPER_SOLAR
from mapper_model.empty_mapper import EmptyMapper
from mapper_model.solar_mappers.solar_daily_mapper import SolarDailyMapper
from mapper_model.solar_mappers.solar_hourly_mapper import SolarHourlyMapper
from mapper_model.precipitation_mappers.precipitation_one_mapper import PrecipitationOneMapper
from mapper_model.kl_mappers.kl_daily_mapper import KlDailyMapper
from mapper_model.station_mapper import StationMapper


class MapperFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_mapper(identifier):
        if identifier == MAPPER_STATION:
            return StationMapper()
        elif identifier == MAPPER_SOLAR:
            return SolarMapper()
        return EmptyMapper()

    @staticmethod
    def get_mapper_for_path(path):
        if Helper.is_solar_daily_file(path):
            return SolarDailyMapper()
        elif Helper.is_solar_hourly_file(path):
            return SolarHourlyMapper()
        elif Helper.is_precipitation_one_file(path):
            return PrecipitationOneMapper()
        elif Helper.is_kl_daily_file(path):
            return KlDailyMapper()
        return EmptyMapper()