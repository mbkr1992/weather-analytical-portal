from parser_model.Parser import Parser
from common.Station import Station
from common.Constants import DOWNLOAD_FOLDER


class StationParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, server_path, filename):
        print('Parsing file: ' + filename);
    
        path_with_filename = DOWNLOAD_FOLDER + server_path + filename

        with open(path_with_filename, encoding='ISO-8859-1') as f:
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