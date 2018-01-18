
from DownloadModel.ConcreteDownloaderFactory import ConcreteDownloaderFactory
from ParserModel.ConcreteParserFactory import ConcreteParserFactory
from Common import Constants, Helper
from DatabaseModel.DBHandler import insert_stations, update_stations
from psycopg2 import errorcodes, IntegrityError

class Operation:
    def __init__(self):
        super().__init__()

    def performOperation(self, factory):
        server = 'ftp-cdc.dwd.de'
        username = 'anonymous'
        password = 'anonymous'
        path = '/pub/CDC/observations_germany/climate/hourly/solar/'
        filename = 'ST_Stundenwerte_Beschreibung_Stationen.txt'

        downloader = factory.getDownloader(Constants.FTP_DOWNLOADER)
        downloader.download(server, username, password, path, filename)

        parser = ConcreteParserFactory().getParser(Constants.STATION)
        stations = parser.parse(filename)
        try:
            insert_stations(stations)
        except Exception as e:
            if e.pgcode == errorcodes.UNIQUE_VIOLATION:
                update_stations(stations)
        else:
            Helper.remove_file(filename)


def main():
    downloader = ConcreteDownloaderFactory()
    Operation().performOperation(downloader)

main()