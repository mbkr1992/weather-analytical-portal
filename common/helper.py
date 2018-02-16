import fnmatch
import os
import zipfile
import gzip
from constants.constants import DOWNLOAD_FOLDER
from itertools import chain
import shutil
from pathlib import Path

class Helper:
    def __init__(self):
        super().__init__()

    @staticmethod
    def unzip(path):
        server_path, separator, filename = path.rpartition('/')
        extracted_path, file_path = None, None
        if path.endswith('.zip'):
            extracted_path = unzip(server_path + separator, filename)
            file_path = find('produkt_*.txt', extracted_path)
        elif path.endswith('.gz'):
            extracted_path, file_path = gunzip(server_path + separator, filename)

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
    def is_solar_10minutes_file(path):
        return '/10_minutes/solar/' in path and path.endswith('.zip')

    @staticmethod
    def is_precipitation_1minute_file(path):
        return '/1_minute/precipitation/' in path and path.endswith('.zip')

    @staticmethod
    def is_precipitation_hourly_file(path):
        return '/hourly/precipitation/' in path and path.endswith('.zip')

    @staticmethod
    def is_precipitation_10minutes_file(path):
        return '/10_minutes/precipitation/' in path and path.endswith('.zip')

    @staticmethod
    def is_kl_daily_file(path):
        return '/daily/kl/' in path and path.endswith('.zip')

    @staticmethod
    def is_kl_monthly_file(path):
        return '/monthly/kl/' in path and path.endswith('.zip')

    @staticmethod
    def is_more_precip_daily_file(path):
        return '/daily/more_precip/' in path and path.endswith('.zip')

    @staticmethod
    def is_more_precip_monthly_file(path):
        return '/monthly/more_precip/' in path and path.endswith('.zip')

    @staticmethod
    def is_soil_temperature_daily(path):
        return '/daily/soil_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_soil_daily(path):
        return '/daily/soil/' in path and path.endswith('.gz')

    @staticmethod
    def is_soil_temperature_hourly(path):
        return '/hourly/soil_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_water_equiv_daily(path):
        return '/daily/water_equiv/' in path and path.endswith('.zip')

    @staticmethod
    def is_air_temperature_hourly(path):
        return '/hourly/air_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_air_temperature_10minutes(path):
        return '/10_minutes/air_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_extreme_temperature_10minutes(path):
        return '/10_minutes/extreme_temperature/' in path and path.endswith('.zip')

    @staticmethod
    def is_cloudiness_hourly(path):
        return '/hourly/cloudiness/' in path and path.endswith('.zip')

    @staticmethod
    def is_wind_hourly(path):
        return '/hourly/wind/' in path and path.endswith('.zip')

    @staticmethod
    def is_wind_10minutes(path):
        return '/10_minutes/wind/' in path and path.endswith('.zip')

    @staticmethod
    def is_extreme_wind_10minutes(path):
        return '/10_minutes/extreme_wind/' in path and path.endswith('.zip')

    @staticmethod
    def is_sun_hourly(path):
        return '/hourly/sun/' in path and path.endswith('.zip')

    @staticmethod
    def is_pressure_hourly(path):
        return '/hourly/pressure/' in path and path.endswith('.zip')

    @staticmethod
    def is_visibility_hourly(path):
        return '/hourly/visibility/' in path and path.endswith('.zip')

    @staticmethod
    def is_analysis_cams_nrealtime(path):
        return path.endswith('.grib')  # 'output.an.[time].grib' in path and path.endswith('.grib')

    @staticmethod
    def is_forecast_cams_nrealtime(path):
        return path.endswith('.grib')  # 'output.fc.[time].grib' in path and path.endswith('.grib')

    @staticmethod
    def is_txt_file(path):
        return path.endswith('.txt')

    @staticmethod
    def is_station(path):
        return 'Beschreibung_Stationen' in path and path.endswith('.txt')

    @staticmethod
    def is_path_parseable(path):
        return (path.endswith('.zip')
                or path.endswith('.txt')
                or path.endswith('.txt.gz')
                or path.endswith('.grib'))


def gunzip(path, filename):
    path_with_filename = DOWNLOAD_FOLDER + path + filename
    with gzip.GzipFile(path_with_filename, "r") as input_file_stream:
        directory, separator, extension = filename.rpartition('.')
        path_to_extracted_folder = DOWNLOAD_FOLDER + path + directory
        path_to_extracted_file = path_to_extracted_folder + '/' + directory

        path = Path(path_to_extracted_folder)
        path.mkdir(parents=True, exist_ok=True)

        out_file_stream = open(path_to_extracted_file, 'wb')
        out_file_stream.write(input_file_stream.read())
        out_file_stream.close()
        input_file_stream.close()

        return path_to_extracted_folder, path_to_extracted_file


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