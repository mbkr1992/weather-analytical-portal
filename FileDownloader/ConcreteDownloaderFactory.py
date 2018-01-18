from FileDownloader.FTPFileDownloader import FTPFileDownloader
from FileDownloader.DownloaderFactory import DownloaderFactory
from Common import Constants

class ConcreteDownloaderFactory(DownloaderFactory):
    def __init__(self):
        super().__init__()

    def getDownloader(self, identifier):
        if identifier == Constants.FTP_DOWNLOADER:
            return FTPFileDownloader()
        return None