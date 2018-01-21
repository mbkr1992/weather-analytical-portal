
from download_model.ConcreteDownloaderFactory import ConcreteDownloaderFactory
from parser_model.ConcreteParserFactory import ConcreteParserFactory
from common import Constants, Helper
from database_model.DBHandler import insert_stations, update_stations
from psycopg2 import errorcodes, IntegrityError
import os


class Operation:
    def __init__(self):
        super().__init__()
        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    def perform_simple_operation(self, factory):
        path = '/pub/CDC/observations_germany/climate/hourly/solar/ST_Stundenwerte_Beschreibung_Stationen.txt'
        server_path, separator, filename = path.rpartition('/')

        downloader = factory.getDownloader(Constants.FTP_DOWNLOADER)
        downloader.download(self.server, self.username, self.password, server_path + separator, filename)

        parser = ConcreteParserFactory().getParser(Constants.STATION)
        stations = parser.parse(server_path + separator, filename)

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

        downloader = factory.getDownloader(Constants.FTP_DOWNLOADER)
        downloader.download(self.server, self.username, self.password, server_path + separator, filename)
        pass


def main():
    downloader = ConcreteDownloaderFactory()
    Operation().perform_zip_operation(downloader)

main()