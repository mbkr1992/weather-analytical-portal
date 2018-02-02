import os, asyncio, aioftp
from ftplib import FTP
from constants.constants import DOWNLOAD_FOLDER
from pathlib import Path
from download_model.downloader import Downloader
from model.file import File
import time
import pathlib


class FTPAsyncFileDownloader(Downloader):
    def __init__(self):
        super().__init__()

        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    async def download(self, prefix_path, file_path):
        async with aioftp.ClientSession(host=self.server, user=self.username, password=self.password) as client:
            await client.change_directory(prefix_path)

            path_for_download = pathlib.PurePosixPath(file_path.replace(prefix_path, ''))

            _, _, filename = file_path.rpartition('/')
            path_for_destination = DOWNLOAD_FOLDER + file_path.replace(filename, '')

            try:
                if not os.path.isfile(file_path):
                    path = Path(path_for_destination)
                    path.mkdir(parents=True, exist_ok=True)

                await client.download(source=path_for_download, destination=path_for_destination, write_into=False)
            except Exception as e:
                print('Exception 2 {0} on path {1}'.format(e, file_path))

    async def download_meta(self, prefix_path):
        async with aioftp.ClientSession(host=self.server, user=self.username, password=self.password) as client:
            await client.change_directory(prefix_path)
            list = await client.list(recursive=True)

            def to_file(file_tuple):
                path, meta_information = file_tuple
                full_path = '{0}{1}'.format(prefix_path, path)
                return File(filename=path.name, path=full_path, meta_information=meta_information)

            return [to_file(item) for item in list if item[1]['type'] == 'file']
