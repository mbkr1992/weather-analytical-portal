
from common import helper
from constants import constants
from download_model.concrete_downloader_factory import ConcreteDownloaderFactory
from mapper_model.mapper_factory import MapperFactory
from parser_model.concrete_parser_factory import ConcreteParserFactory


class Operation:
    def __init__(self):
        super().__init__()
        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    def perform_simple_operation(self, factory):
        path = '/pub/CDC/observations_germany/climate/hourly/solar/ST_Stundenwerte_Beschreibung_Stationen.txt'
        server_path, separator, filename = path.rpartition('/')

        downloader = factory.getDownloader(constants.FTP_DOWNLOADER)
        downloader.download(self.server, self.username, self.password, server_path + separator, filename)

        mapper = MapperFactory().get_mapper(constants.STATION)

        parser = ConcreteParserFactory().get_parser(constants.STATION)
        path_with_filename = server_path + separator + filename

        stations = parser.parse(path_with_filename, mapper)

        # try:
        #     insert_stations(stations)
        # except Exception as e:
        #     # Entries already in database, so just update them
        #     if e.pgcode == errorcodes.UNIQUE_VIOLATION:
        #         update_stations(stations)
        # else:
        #     Helper.remove_file(filename)

    def perform_zip_operation(self, factory):
        path = '/pub/CDC/observations_germany/climate/hourly/solar/stundenwerte_ST_02928_row.zip'
        server_path, separator, filename = path.rpartition('/')

        downloader = factory.getDownloader(constants.FTP_DOWNLOADER)
        downloader.download(self.server, self.username, self.password, server_path + separator, filename)

        extracted_path = helper.unzip(server_path + separator, filename)
        file_path = helper.find('produkt_*.txt', extracted_path)

        mapper = MapperFactory().get_mapper(constants.SOLAR)
        parser = ConcreteParserFactory().get_parser(constants.SIMPLE)
        parser.parse(file_path, mapper)
        pass


def main():
    downloader = ConcreteDownloaderFactory()
    Operation().perform_zip_operation(downloader)

main()