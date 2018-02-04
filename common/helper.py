import fnmatch
import os
import zipfile

from constants.constants import DOWNLOAD_FOLDER
from itertools import chain
import shutil


class Helper:
    def __init__(self):
        super().__init__()

    @staticmethod
    def unzip(path):
        server_path, separator, filename = path.rpartition('/')

        extracted_path = unzip(server_path + separator, filename)
        file_path = find('produkt_*.txt', extracted_path)
        return extracted_path, file_path

    @staticmethod
    def remove_directory(path):
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
        pass

    @staticmethod
    def flatmap(f, items):
        return chain.from_iterable(map(f, items))

    @staticmethod
    def is_solar_hourly_file(path):
        return '/hourly/solar/' in path and path.endswith('.zip')

    @staticmethod
    def is_solar_daily_file(path):
        return '/daily/solar/' in path and path.endswith('.zip')

    @staticmethod
    def is_precipitation_one_file(path):
        return '/1_minute/precipitation/' in path and path.endswith('.zip')

    @staticmethod
    def is_kl_daily_file(path):
        return '/daily/kl/' in path and path.endswith('.zip')

    @staticmethod
    def is_more_precip_daily_file(path):
        return '/daily/more_precip/' in path and path.endswith('.zip')

    @staticmethod
    def is_soil_temperature_daily(path):
        return '/daily/soil_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_water_equiv_daily(path):
        return '/daily/water_equiv/' in path and path.endswith('.zip')

    @staticmethod
    def is_path_parseable(path):
        return not (path.endswith('.pdf')
                    or path.endswith('.txt')
                    or path.endswith('.html')
                    or path.endswith('.gz'))

def unzip(path, filename):
    # taken from https://stackoverflow.com/questions/3451111/unzipping-files-in-python
    path_with_filename = DOWNLOAD_FOLDER + path + filename

    with zipfile.ZipFile(path_with_filename, "r") as zip_ref:
        directory, separator, extension = filename.rpartition('.')
        path_to_files = DOWNLOAD_FOLDER + path + directory

        zip_ref.extractall(path_to_files)
        return path_to_files


def find(pattern, path):
    # taken from https://stackoverflow.com/questions/1724693/find-a-file-in-python
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))

    # return the first result
    return result.pop(0)