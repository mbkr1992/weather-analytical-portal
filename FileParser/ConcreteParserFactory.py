from FileParser.FileSoilStationParser import FileSoilStationParser
from FileParser.ParserFactory import ParserFactory
from FileParser.StationParser import StationParser
from Common import Constants

class ConcreteParserFactory(ParserFactory):
    def __init__(self):
        super().__init__()

    def getParser(self, identifier):
        if identifier == Constants.SOIL_STATION:
            return FileSoilStationParser()
        elif identifier == Constants.STATION:
            return StationParser()
        return None