import os
from ftplib import FTP
from pathlib import Path

from constants.constants import DOWNLOAD_FOLDER
from download_model.downloader import Downloader


class FTPFileDownloader(Downloader):
    def __init__(self):
        super().__init__()

    def download(self, server, username, password, server_path, filename, force_download=False):
        print('Downloading file: ' + filename);
        ftp = FTP(server)
        ftp.login(username, password)
        ftp.cwd(server_path)
        path_without_filename = DOWNLOAD_FOLDER + server_path
        path_with_filename = DOWNLOAD_FOLDER + server_path + filename

        if not os.path.isfile(path_with_filename) or force_download:
            path = Path(path_without_filename)
            path.mkdir(parents=True, exist_ok=True)

            with open(path_with_filename, 'wb') as local_file:
                ftp.retrbinary('RETR ' + filename, local_file.write, 1024)
                local_file.close()
        else:
            print('File already downloaded')
        ftp.quit()
