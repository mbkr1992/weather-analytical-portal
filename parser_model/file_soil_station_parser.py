from model import station
from parser_model.parser import Parser


class FileSoilStationParser(Parser):

    def __init__(self):
        super().__init__()

    def parse(self, server_path, filename):
        print('Parsing file: ' + filename);
        with open(filename, encoding='ISO-8859-1') as f:
            listOfStations = []
            keys = f.readline().split(';')
            for line in f:
                values = line.split(';')
                station = {}
                for i in range(0, len(keys)):
                    station[keys[i].strip()] = values[i].strip()
                listOfStations.append(toStation(station))
            f.close()
            return listOfStations


def toStation(station) -> station:
    stationIndex = station['Stationsindex']
    height = station['Höhe in m']

    return None
    pass