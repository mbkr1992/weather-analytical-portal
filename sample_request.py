
from common import helper
from constants import constants
from download_model.downloader_factory import DownloaderFactory
from itertools import islice
from common.ftp_helper import FTPHelper
from mapper_model.mapper_factory import MapperFactory
from parser_model.parser_factory import ParserFactory
from database_model.db_handler import insert_solar_data, select_files, update_file, insert_files, select_files_simple
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
    # try:
    # paths = [] #select_files_simple()
    prefix_path = '/pub/CDC/observations_germany/climate/'
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(fetch_path_operation(prefix_path))
    # print('Operation One done');
    start_time = time.time()
    sem = asyncio.Semaphore(value=10)

    # Fetch all the non-downloaded file paths from database
    paths_to_download = select_files_simple()
    print('Fetching from database {0}'.format(len(paths_to_download)));
    download_operations = [download_file_operation(sem, prefix_path, file_path) for file_path in paths_to_download]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(download_operations))
    loop.close()

    print('Operation Two done');
    print('{0} seconds'.format(time.time() - start_time))

    # except Exception as e:
    #     print('Exception: {0}'.format(e))
    pass

def perform_random():
    seq = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    limit = 10
    print([seq[i::limit] for i in range(limit)])


# def perform_operation():
#     async def batch_operation(the_path):
#         try:
#             downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP_ASYNC)
#             await downloader.download(the_path)
#             # update_file(the_path)
#         except Exception as ee:
#             print('Exception: {0}'.format(ee))

 # limit_of_threads = 10
    # list_of_sub_operations = [download_operations[i::10] for i in range(limit_of_threads)]
    #
    # for sub_operations in list_of_sub_operations:


def main():

    perform_async_operation()
    # perform_random()

main()