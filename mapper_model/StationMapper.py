from mapper_model.Mapper import Mapper
from model.Station import Station


class StationMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, station):
        return Station(station)