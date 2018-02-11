from common.helper import Helper
from mapper_model.cloudiness.cloudiness_hourly_mapper import CloudinessHourlyMapper
from mapper_model.empty_mapper import EmptyMapper
from mapper_model.kl.kl_daily_mapper import KlDailyMapper
from mapper_model.kl.kl_monthly_mapper import KlMonthlyMapper
from mapper_model.more_precip.more_precip_daily_mapper import MorePrecipDailyMapper
from mapper_model.more_precip.more_precip_monthly_mapper import MorePrecipMonthlyMapper
from mapper_model.precipitation.precipitation_10minute_minute import Precipitation10MinuteMapper
from mapper_model.precipitation.precipitation_1minute_mapper import Precipitation1MinuteMapper
from mapper_model.precipitation.precipitation_hourly_mapper import PrecipitationHourlyMapper
from mapper_model.pressure.pressure_hourly_mapper import PressureHourlyMapper
from mapper_model.soil.soil_temperature.soil_temperature_daily_mapper import SoilTemperatureDailyMapper
from mapper_model.soil.soil_temperature.soil_temperature_hourly_mapper import SoilTemperatureHourlyMapper
from mapper_model.soil.soil_and_crops.soil import SoilTemperature
from mapper_model.solar.solar_10minute_mapper import Solar10MinuteMapper
from mapper_model.solar.solar_daily_mapper import SolarDailyMapper
from mapper_model.solar.solar_hourly_mapper import SolarHourlyMapper
from mapper_model.station.station_mapper import StationMapper
from mapper_model.sun.sun_hourly_mapper import SunHourlyMapper
from mapper_model.temperature.air_temperature.air_temperature_10minute_mapper import AirTemperature10MinuteMapper
from mapper_model.temperature.air_temperature.air_temperature_hourly_mapper import AirTemperatureHourlyMapper
from mapper_model.temperature.exteme_temperature.extreme_temperature_10minute_mapper import \
    ExtremeTemperature10MinuteMapper
from mapper_model.visibility.visibility_hourly_mapper import VisibilityHourlyMapper
from mapper_model.water_equiv.water_equiv_daily_mapper import WaterEquivDailyMapper
from mapper_model.wind.wind_10minute_mapper import Wind10MinuteMapper
from mapper_model.wind.wind_extreme_10minute_mapper import WindExtreme10MinuteMapper
from mapper_model.wind.wind_hourly_mapper import WindHourlyMapper


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
        elif Helper.is_solar_10minutes_file(path):
            return Solar10MinuteMapper()
        elif Helper.is_sun_hourly(path):
            return SunHourlyMapper()

        elif Helper.is_precipitation_1minute_file(path):
            return Precipitation1MinuteMapper()
        elif Helper.is_precipitation_hourly_file(path):
            return PrecipitationHourlyMapper()
        elif Helper.is_precipitation_10minutes_file(path):
            return Precipitation10MinuteMapper()

        elif Helper.is_kl_daily_file(path):
            return KlDailyMapper()
        elif Helper.is_kl_monthly_file(path):
            return KlMonthlyMapper()

        elif Helper.is_more_precip_daily_file(path):
            return MorePrecipDailyMapper()
        elif Helper.is_more_precip_monthly_file(path):
            return MorePrecipMonthlyMapper()

        elif Helper.is_soil_temperature_daily(path):
            return SoilTemperatureDailyMapper()
        elif Helper.is_soil_temperature_hourly(path):
            return SoilTemperatureHourlyMapper()
        elif Helper.is_soil_daily(path):
            return SoilTemperature()

        elif Helper.is_water_equiv_daily(path):
            return WaterEquivDailyMapper()

        elif Helper.is_air_temperature_hourly(path):
            return AirTemperatureHourlyMapper()
        elif Helper.is_air_temperature_10minutes(path):
            return AirTemperature10MinuteMapper()

        elif Helper.is_wind_hourly(path):
            return WindHourlyMapper()
        elif Helper.is_extreme_wind_10minutes(path):
            return WindExtreme10MinuteMapper()
        elif Helper.is_wind_10minutes(path):
            return Wind10MinuteMapper()

        elif Helper.is_cloudiness_hourly(path):
            return CloudinessHourlyMapper()

        elif Helper.is_visibility_hourly(path):
            return VisibilityHourlyMapper()

        elif Helper.is_pressure_hourly(path):
            return PressureHourlyMapper()

        elif Helper.is_extreme_temperature_10minutes(path):
            return ExtremeTemperature10MinuteMapper()

        elif Helper.is_station(path):
            return StationMapper()

        return EmptyMapper()