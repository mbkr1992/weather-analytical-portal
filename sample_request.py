
from common import helper
from constants import constants
from download_model.downloader_factory import DownloaderFactory
from common.ftp_helper import FTPHelper
from mapper_model.mapper_factory import MapperFactory
from parser_model.parser_factory import ParserFactory
from database_model.db_handler import insert_solar_data, select_files, update_file, insert_files, select_files_simple
from operation_model.operation_factory import OperationFactory
import asyncio

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
#
# print(server)
urls = [1, 2, 3, 4, 5]

async def call_url(iid):
    for i in range(1, 5):
        await asyncio.sleep(0.1)
        print('Starting {0}:{1}'.format(iid, i))
    pass


def perform_async_operation():
    async def batch_operation(the_path):
        try:
            downloader = DownloaderFactory.get_downloader(constants.DOWNLOADER_FTP_ASYNC)
            await downloader.download(the_path)
            update_file(the_path)
        except Exception as ee:
            print('Exception: {0}'.format(ee))

    try:
        paths = select_files_simple()
        furute = [batch_operation(paths[0])]
        # futures = [batch_operation(path) for path in paths]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(furute))
        loop.close()

    except Exception as e:
        print('Exception: {0}'.format(e))
    pass


def main():
    # path = '/pub/CDC/observations_germany/climate/'
    # list_of_files = FTPHelper().fetch_all_meta_information(path=path)
    # insert_files(list_of_files)

    perform_async_operation()

main()

# data = [(1,'x'), (2,'y')]
# insert_query = 'insert into t (a, b) values %s'
# psycopg2.extras.execute_values (
#     cursor, insert_query, data, template=None, page_size=100
# )