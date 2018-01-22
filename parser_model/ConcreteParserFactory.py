from parser_model.FileSoilStationParser import FileSoilStationParser
from parser_model.ParserFactory import ParserFactory
from parser_model.StationParser import StationParser
from parser_model.SimpleParser import SimpleParser
from common import Constants


class ConcreteParserFactory(ParserFactory):
    def __init__(self):
        super().__init__()

    def get_parser(self, identifier):
        if identifier == Constants.SOIL_STATION:
            return FileSoilStationParser()
        elif identifier == Constants.STATION:
            return StationParser()
        elif identifier == Constants.SIMPLE:
            return SimpleParser()
        return None
