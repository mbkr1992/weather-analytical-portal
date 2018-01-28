import os
from ftplib import FTP
from model.file import File
from constants.constants import DOWNLOAD_FOLDER
from pathlib import Path
from download_model.downloader import Downloader


class FTPHelper:
    def __init__(self):
        super().__init__()

        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    def fetch_all_meta_information(self, path='/pub/CDC/observations_germany/climate/'):
        ftp = FTP(self.server)
        ftp.login(self.username, self.password)
        ftp.cwd(path)
        ls = ftp.mlsd()

        files = list()
        for entry in ls:
            folder, meta_information = entry
            if is_directory(entry):
                if is_main_directory(entry):
                    files.extend(self.fetch_all_meta_information('{0}{1}/'.format(path, folder)))
            else:
                file = File('{0}{1}'.format(path, folder), meta_information)
                files.append(file)

        return files

    def download_file(self, path):
        ftp = FTP(self.server)
        ftp.login(self.username, self.password)
        server_path, separator, filename = path.rpartition('/')
        ftp.cwd(server_path)

        path_without_filename = DOWNLOAD_FOLDER + server_path
        path_with_filename = DOWNLOAD_FOLDER + server_path + separator + filename

        if not os.path.isfile(path_with_filename):
            path = Path(path_without_filename)
            path.mkdir(parents=True, exist_ok=True)

            with open(path_with_filename, 'wb') as local_file:
                ftp.retrbinary('RETR ' + filename, local_file.write, 1024)
                local_file.close()
        else:
            print('File already downloaded')
        ftp.quit()


def is_main_directory(entry):
    value, meta = entry
    return meta['type'] == 'dir'


def is_directory(entry):
    value, meta = entry
    return meta['type'] == 'dir' or meta['type'] == 'cdir' or meta['type'] == 'pdir'


def exclude(entry):
    value, meta = entry
    if meta['type'] == 'cdir':
        return True
    return False