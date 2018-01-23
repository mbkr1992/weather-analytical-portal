from constants.constants import STATION, SOLAR
from mapper_model.solar_mapper import SolarMapper
from mapper_model.station_mapper import StationMapper


class MapperFactory:
    def __init__(self):
        super().__init__()

    def get_mapper(self, identifier):
        if identifier == STATION:
            return StationMapper()
        elif identifier == SOLAR:
            return SolarMapper()