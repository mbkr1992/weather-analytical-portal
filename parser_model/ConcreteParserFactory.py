from parser_model.FileSoilStationParser import FileSoilStationParser
from parser_model.ParserFactory import ParserFactory
from parser_model.StationParser import StationParser
from common import Constants

class ConcreteParserFactory(ParserFactory):
    def __init__(self):
        super().__init__()

    def getParser(self, identifier):
        if identifier == Constants.SOIL_STATION:
            return FileSoilStationParser()
        elif identifier == Constants.STATION:
            return StationParser()
        return None