from ParserModel.Parser import Parser
from Common.Station import Station

class StationParser(Parser):

    def __init__(self):
        super().__init__()

    def parse(self, filename):
        print('Parsing file: ' + filename);
        with open(filename, encoding='ISO-8859-1') as f:
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