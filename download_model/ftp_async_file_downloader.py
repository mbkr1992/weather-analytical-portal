import os, asyncio, aioftp
from ftplib import FTP
from constants.constants import DOWNLOAD_FOLDER
from pathlib import Path
from download_model.downloader import Downloader


class FTPAsyncFileDownloader(Downloader):
    def __init__(self):
        super().__init__()

        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    async def download(self, path):
        async with aioftp.ClientSession(host=self.server, user=self.username, password=self.password) as client:
            # local_path = DOWNLOAD_FOLDER + path
            file_path, separator, filename = path.rpartition('/')


            print('Change directory {0}, {1}, {2}'.format(file_path, separator, filename))
            # await client.change_directory(path='/pub/CDC/observations_germany/climate/1_minute/precipitation/historical/2017')

            source = '/pub/CDC/observations_germany/climate/'
            destination = 'downloads/pub/CDC/observations_germany/climate/'
            # for p, info in (await client.list()):
            #     print('stats for client:', p, info)
            try:
                result2 = await client.download(source=source, destination=destination, write_into='true')
                print('Download file {0}'.format(1))
            except Exception as e:
                print('Exception {0}'.format(e))



        # ftp = FTP(self.server)
        # ftp.login(self.username, self.password)
        # server_path, separator, filename = path.rpartition('/')
        # ftp.cwd(server_path)
        #
        # path_without_filename = DOWNLOAD_FOLDER + server_path
        # path_with_filename = DOWNLOAD_FOLDER + server_path + separator + filename
        #
        # if not os.path.isfile(path_with_filename):
        #     path = Path(path_without_filename)
        #     path.mkdir(parents=True, exist_ok=True)
        #
        #     with open(path_with_filename, 'wb') as local_file:
        #         ftp.retrbinary('RETR ' + filename, local_file.write, 1024)
        #         local_file.close()
        #         print('File downloaded')
        # else:
        #     print('File already downloaded')
        # ftp.quit()


