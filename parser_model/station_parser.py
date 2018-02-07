from constants.constants import DOWNLOAD_FOLDER
from model.station import Station
from parser_model.parser import Parser
import re


class StationParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path, mapper):

        temporary_path = DOWNLOAD_FOLDER + path

        with open(temporary_path, encoding='ISO-8859-1') as f:
            list_of_stations = []
            try:

                # Headings Station_id etc
                keys = f.readline().split()

                # Partition line ------- -------
                f.readline()

                # Actual values 00183 etc
                for line in f:
                    values = line.split()

                    # if values are empty or bogus ignore them
                    if not values or stray_txt_found(values):
                        continue

                    station = {}
                    for i in range(0, len(keys)):
                        station[keys[i]] = values[i]
                    list_of_stations.append(mapper.map(station))
            except Exception as e:
                print('Station parsing error {0}, '.format(e))
            finally:
                f.close()
            return list_of_stations


def stray_txt_found(values):
    return 'ST_ID' in values or '-----' in values or 'Stations_id' in values or '-----------' in values
