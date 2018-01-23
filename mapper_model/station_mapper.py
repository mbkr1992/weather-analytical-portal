from mapper_model.mapper import Mapper
from model.station import Station


class StationMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, station):
        return Station(station)