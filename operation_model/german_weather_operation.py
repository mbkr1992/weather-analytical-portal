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

            # paths_to_parse = select_non_parsed_files()

            paths_to_parse = [
                '/pub/CDC/observations_germany/climate/1_minute/precipitation/historical/1993/1minutenwerte_nieder_00003_19930701_19930731_hist.zip',
                # '/pub/CDC/observations_germany/climate/10_minutes/air_temperature/historical/10minutenwerte_tu_00003_19930428_19991231_hist.zip',
                # '/pub/CDC/observations_germany/climate/10_minutes/extreme_temperature/historical/10minutenwerte_tx_00003_19930428_19991231_hist.zip',
                # '/pub/CDC/observations_germany/climate/10_minutes/extreme_wind/historical/10minutenwerte_fx_00003_19930428_19991231_hist.zip',
                # '/pub/CDC/observations_germany/climate/10_minutes/precipitation/historical/10minutenwerte_rr_00003_19930428_19991231_hist.zip',
                # '/pub/CDC/observations_germany/climate/10_minutes/solar/historical/10minutenwerte_solar_00003_19930428_19991231_hist.zip'
                # '/pub/CDC/observations_germany/climate/10_minutes/wind/historical/10minutenwerte_ff_00003_19930428_19991231_hist.zip',
                # '/pub/CDC/observations_germany/climate/daily/kl/historical/tageswerte_KL_00001_19370101_19860630_hist.zip',
                # '/pub/CDC/observations_germany/climate/daily/more_precip/historical/tageswerte_RR_00001_19120101_19860630_hist.zip',
                # '/pub/CDC/observations_germany/climate/daily/soil_temperature/historical/tageswerte_EB_00003_19510101_20110331_hist.zip',
                # '/pub/CDC/observations_germany/climate/daily/solar/tageswerte_ST_00183_row.zip',
                # '/pub/CDC/observations_germany/climate/daily/water_equiv/historical/tageswerte_Wa_00001_19370130_19580125_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/air_temperature/historical/stundenwerte_TU_00003_19500401_20110331_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/cloudiness/historical/stundenwerte_N_00003_19500401_20110401_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/precipitation/historical/stundenwerte_RR_00003_19500401_20110401_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/pressure/historical/stundenwerte_P0_00003_19500401_20110401_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/soil_temperature/historical/stundenwerte_EB_00003_19510101_20110331_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/solar/stundenwerte_ST_00183_row.zip',
                # '/pub/CDC/observations_germany/climate/hourly/sun/historical/stundenwerte_SD_00003_19510101_20110331_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/visibility/historical/stundenwerte_VV_00003_19500401_20110401_hist.zip',
                # '/pub/CDC/observations_germany/climate/hourly/wind/historical/stundenwerte_FF_00003_19370101_20110331_hist.zip',
                # '/pub/CDC/observations_germany/climate/monthly/kl/historical/monatswerte_KL_00001_19310101_19860630_hist.zip',
                # '/pub/CDC/observations_germany/climate/monthly/more_precip/historical/monatswerte_RR_00001_18910101_19860630_hist.zip',
                # '/pub/CDC/observations_germany/climate/subdaily/standard_format/kl_10XXX_bis_1999.txt.gz'
            ];

            if paths_to_parse:
                for path in paths_to_parse:
                    folder_path, file_path = None, None

                    try:
                        if not Helper.is_path_parseable(path=path):
                            continue

                        if Helper.is_station(path):
                            file_path = path
                        else:
                            folder_path, file_path = Helper.unzip(path)

                        parser = ParserFactory.get_parser_for_path(path)
                        mapper = MapperFactory.get_mapper_for_path(path)
                        # print('4. Parser {0}, Mapper: {1}'.format(parser, mapper))

                        items = parser.parse(path=file_path, mapper=mapper)

                        print('5. Path file {0}, items: {1}'.format(path, len(items)))

                        mapper.insert_items(items)
                        # mapper.update_file_parsed_flag(path)
                    except Exception as e:
                        print('Exception: {0}, {1}'.format(e, path))
                    finally:
                        if folder_path:
                            Helper.remove_directory(folder_path)

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
