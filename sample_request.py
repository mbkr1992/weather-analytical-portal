
from common import helper
from constants import constants
from download_model.downloader_factory import DownloaderFactory
from itertools import islice
from common.ftp_helper import FTPHelper
from mapper_model.mapper_factory import MapperFactory
from parser_model.parser_factory import ParserFactory
from database_model.db_handler import insert_solar_data, update_file, insert_files, select_files_simple
from operation_model.operation_factory import OperationFactory
import asyncio
import time
from itertools import islice

# from ecmwfapi import ECMWFDataServer
#
# server = ECMWFDataServer()
# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2017-10-01/to/2017-10-25",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "167.128",
#     "step": "0
#     "stream": "oper",
#     "time": "00:00:00",
#     "type": "an",
#     "target": "output",
# })


def perform_async_operation():
    async def fetch_path_operation(prefix_path):

        downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP_ASYNC)

        # Download all the files and meta information
        file_paths = await downloader.download_meta(prefix_path)

        # Store all the meta information in the database
        insert_files(file_paths)

    async def download_file_operation(semaphore, prefix_path, file_path):
        try:
            await semaphore.acquire()

            # start downloading the files here
            downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP_ASYNC)
            await downloader.download(prefix_path, file_path)
            update_file(file_path)

        except Exception as e:
            print('Exception 1 {0} on path {1}'.format(e.strerror, file_path))
        finally:
            semaphore.release()

        # print('File downloaded', file_path);

    print('Operation One start');
    try:
        # Fetch all the paths from the duetsche service
        prefix_path = '/pub/CDC/observations_germany/climate/daily/solar/'
        loop = asyncio.get_event_loop()
        loop.run_until_complete(fetch_path_operation(prefix_path))
        # loop.close()
        print('Operation One done');


        # Fetch all the non-downloaded file paths from database
        paths_to_download = select_files_simple()
        sem = asyncio.Semaphore(value=10)
        download_operations = [download_file_operation(sem, prefix_path, file_path) for file_path in paths_to_download]

        # loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(download_operations))
        loop.close()

    except Exception as e:
        print('Exception: {0}'.format(e))
    pass


def main():
    perform_async_operation()

main()