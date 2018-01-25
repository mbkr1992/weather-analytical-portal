from constants import constants
from parser_model.file_soil_station_parser import FileSoilStationParser
from parser_model.simple_parser import SimpleParser
from parser_model.station_parser import StationParser


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
        return None
