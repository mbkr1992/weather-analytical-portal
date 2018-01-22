from common.Constants import DOWNLOAD_FOLDER
from model.Station import Station
from parser_model.Parser import Parser


class StationParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path_file):
        print('Parsing file: ' + path_file);

        temporary_path = DOWNLOAD_FOLDER + path_file

        with open(temporary_path, encoding='ISO-8859-1') as f:
            list_of_stations = []

            # Headings Station_id etc
            keys = f.readline().split()

            # Partition line ------- -------
            f.readline()

            # Actual values 00183 etc
            for line in f:
                values = line.split()
                station = {}
                for i in range(0, len(keys)):
                    station[keys[i]] = values[i]
                list_of_stations.append(Station(station))
            f.close()
            return list_of_stations
