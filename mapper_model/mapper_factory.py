from common.helper import Helper
from mapper_model.empty_mapper import EmptyMapper
from mapper_model.solar.solar_daily_mapper import SolarDailyMapper
from mapper_model.solar.solar_hourly_mapper import SolarHourlyMapper
from mapper_model.precipitation.precipitation_one_mapper import PrecipitationOneMapper
from mapper_model.more_precip.more_precip_daily_mapper import MorePrecipDailyMapper
from mapper_model.soil_temperature.soil_temperature_daily_mapper import SoilTemperatureDailyMapper
from mapper_model.kl.kl_daily_mapper import KlDailyMapper
from mapper_model.water_equiv.water_equiv_daily_mapper import WaterEquivDailyMapper


class MapperFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_mapper(identifier):
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
        elif Helper.is_more_precip_daily_file(path):
            return MorePrecipDailyMapper()
        elif Helper.is_soil_temperature_daily(path):
            return SoilTemperatureDailyMapper()
        elif Helper.is_water_equiv_daily(path):
            return WaterEquivDailyMapper()
        return EmptyMapper()