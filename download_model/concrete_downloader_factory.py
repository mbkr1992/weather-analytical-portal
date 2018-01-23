from constants import constants
from download_model.downloader_factory import DownloaderFactory
from download_model.ftp_file_downloader import FTPFileDownloader


class ConcreteDownloaderFactory(DownloaderFactory):
    def __init__(self):
        super().__init__()

    def getDownloader(self, identifier):
        if identifier == constants.FTP_DOWNLOADER:
            return FTPFileDownloader()
        return None