from common.ftp_helper import FTPHelper
from download_model.downloader import Downloader


class FTPFileDownloader(Downloader):
    def __init__(self):
        super().__init__()

    def download(self, path):
        print('Downloading file: ' + path);
        FTPHelper.download_file(path)
