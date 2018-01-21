
from download_model.ConcreteDownloaderFactory import ConcreteDownloaderFactory
from parser_model.ConcreteParserFactory import ConcreteParserFactory
from common import Constants, Helper
from database_model.DBHandler import insert_stations, update_stations
from psycopg2 import errorcodes, IntegrityError
import os


class Operation:
    def __init__(self):
        super().__init__()

    def perform_operation(self, factory):
        server = 'ftp-cdc.dwd.de'
        username = 'anonymous'
        password = 'anonymous'
        path = '/pub/CDC/observations_germany/climate/hourly/solar/ST_Stundenwerte_Beschreibung_Stationen.txt'
        server_path, separator, filename = path.rpartition('/')

        downloader = factory.getDownloader(Constants.FTP_DOWNLOADER)
        downloader.download(server, username, password, server_path + separator, filename)

        parser = ConcreteParserFactory().getParser(Constants.STATION)
        stations = parser.parse(filename)
        # try:
        #     insert_stations(stations)
        # except Exception as e:
        #     # Entries already in database, so just update them
        #     if e.pgcode == errorcodes.UNIQUE_VIOLATION:
        #         update_stations(stations)
        # else:
        #     Helper.remove_file(filename)


def main():
    downloader = ConcreteDownloaderFactory()
    Operation().perform_operation(downloader)

main()