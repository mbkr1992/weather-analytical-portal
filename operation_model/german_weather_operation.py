from operation_model.operation import Operation
from database_model.db_handler import update_file_download_flag, insert_files, select_non_downloaded_files, select_non_parsed_files
from constants import constants
from download_model.downloader_factory import DownloaderFactory
from parser_model.parser_factory import ParserFactory
from mapper_model.mapper_factory import MapperFactory
from common.helper import Helper

import asyncio


class GermanWeatherOperation(Operation):
    def __init__(self):
        super().__init__()

    def perform_operation(self, prefix_path='/pub/CDC/observations_germany/climate/'):
        try:
            print('GermanWeatherOperation in-progress');
            # # Fetch all the paths from the duetsche service
            # loop = asyncio.get_event_loop()
            # loop.run_until_complete(fetch_path_operation(prefix_path))

            # # Fetch all the non-downloaded file paths from database
            # paths_to_download = select_non_downloaded_files()
            # sem = asyncio.Semaphore(value=10)
            # if paths_to_download:
            #     download_operations = [download_file_operation(sem, prefix_path, file_path) for file_path in
            #                            paths_to_download]
            #     loop.run_until_complete(asyncio.wait(download_operations))

            paths_to_parse = select_non_parsed_files()

            if paths_to_parse:
                for path in paths_to_parse:
                    try:
                        print('Path file {0}'.format(path))
                        if not Helper.is_path_parseable(path=path):
                            continue

                        extracted_path, unzip_file = Helper.unzip(path)
                        parser = ParserFactory.get_parser_for_path(path)
                        mapper = MapperFactory.get_mapper_for_path(path)

                        items = parser.parse(path=unzip_file, mapper=mapper)
                        mapper.insert_items(items)
                        mapper.update_file_parsed_flag(path)
                        Helper.remove_directory(extracted_path)
                    except Exception as e:
                        print('Exception: {0}, {1}'.format(e, path))

            print('GermanWeatherOperation complete');

        except Exception as e:
            print('Exception: {0}'.format(e))

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
        update_file_download_flag(file_path)

    except Exception as e:
        print('Exception 1 {0} on path {1}'.format(e, file_path))
    finally:
        semaphore.release()
