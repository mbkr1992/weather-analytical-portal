
from FileDownloader.ConcreteDownloaderFactory import ConcreteDownloaderFactory
from FileParser.ConcreteParserFactory import ConcreteParserFactory
from Common import Constants
from DBHandler import DBHandler


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
        DBHandler.insert_stations(stations)


def main():
    downloader = ConcreteDownloaderFactory()
    Operation().performOperation(downloader)

main()