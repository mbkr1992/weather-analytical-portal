
from common import helper
from constants import constants
from download_model.downloader_factory import DownloaderFactory
from common.ftp_helper import FTPHelper
from mapper_model.mapper_factory import MapperFactory
from parser_model.parser_factory import ParserFactory
from database_model.db_handler import insert_solar_data, select_files, update_file, insert_files, select_files_simple
from operation_model.operation_factory import OperationFactory
import asyncio
# def perform_simple_operation(factory):
#     path = '/pub/CDC/observations_germany/climate/hourly/solar/ST_Stundenwerte_Beschreibung_Stationen.txt'
#     server_path, separator, filename = path.rpartition('/')
#
#     downloader = factory.get_downloader(constants.FTP_DOWNLOADER)
#     downloader.download(self.server, self.username, self.password, server_path + separator, filename)
#
#     mapper = MapperFactory().get_mapper(constants.STATION)
#
#     parser = ParserFactory().get_parser(constants.STATION)
#     path_with_filename = server_path + separator + filename
#
#     stations = parser.parse(path_with_filename, mapper)
#
#     # try:
#     #     insert_stations(stations)
#     # except Exception as e:
#     #     # Entries already in database, so just update them
#     #     if e.pgcode == errorcodes.UNIQUE_VIOLATION:
#     #         update_stations(stations)
#     # else:
#     #     Helper.remove_file(filename)
def perform_text_operation():
    path = '/pub/CDC/observations_germany/climate/hourly/solar/ST_Stundenwerte_Beschreibung_Stationen.txt'
    mapper = MapperFactory.get_mapper(constants.MAPPER_STATION)
    parser = ParserFactory.get_parser(constants.PARSER_STATION)
    downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP)
    operation = OperationFactory.get_operation(constants.OPERATION_TEXT)
    items = operation.perform_operation(path=path, mapper=mapper, parser=parser, downloader=downloader)


def perform_zip_operation():
    def batch_operation(the_path):
        print('Downloading file {0}'.format(the_path))
        try:
            downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP)
            downloader.download(the_path)
        except Exception as e:
            print('Exception: {0}'.format(e))

    try:
        select_files(batch_operation=batch_operation)
    except Exception as e:
        print('Exception: {0}'.format(e))
    pass

def main():
    perform_async_operation()
    pass

main()