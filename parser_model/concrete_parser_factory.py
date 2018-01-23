from constants import constants
from parser_model.file_soil_station_parser import FileSoilStationParser
from parser_model.parser_factory import ParserFactory
from parser_model.simple_parser import SimpleParser
from parser_model.station_parser import StationParser


class ConcreteParserFactory(ParserFactory):
    def __init__(self):
        super().__init__()

    def get_parser(self, identifier):
        if identifier == constants.SOIL_STATION:
            return FileSoilStationParser()
        elif identifier == constants.STATION:
            return StationParser()
        elif identifier == constants.SIMPLE:
            return SimpleParser()
        return None
