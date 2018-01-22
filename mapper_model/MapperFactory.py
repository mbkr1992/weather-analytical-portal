from common.Constants import STATION, SOLAR
from mapper_model.StationMapper import StationMapper
from mapper_model.SolarMapper import SolarMapper


class MapperFactory:
    def __init__(self):
        super().__init__()

    def get_mapper(self, identifier):
        if identifier == STATION:
            return StationMapper()
        elif identifier == SOLAR:
            return SolarMapper()