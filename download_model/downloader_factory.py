from constants import constants
from download_model.ftp_file_downloader import FTPFileDownloader


class DownloaderFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_downloader(identifier):
        if identifier == constants.DOWNLOADER_FTP:
            return FTPFileDownloader()
        return None