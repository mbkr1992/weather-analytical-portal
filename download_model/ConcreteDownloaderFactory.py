from download_model.FTPFileDownloader import FTPFileDownloader
from download_model.DownloaderFactory import DownloaderFactory
from common import Constants

class ConcreteDownloaderFactory(DownloaderFactory):
    def __init__(self):
        super().__init__()

    def getDownloader(self, identifier):
        if identifier == Constants.FTP_DOWNLOADER:
            return FTPFileDownloader()
        return None