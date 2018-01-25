from constants.constants import MAPPER_STATION, MAPPER_SOLAR
from mapper_model.solar_mapper import SolarMapper
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