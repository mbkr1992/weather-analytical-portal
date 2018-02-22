from operation_model.operation import Operation
from database_model.db_handler import update_file_download_flag, insert_files, select_non_downloaded_files, select_non_parsed_files
from constants import constants
from parser_model.parser_factory import ParserFactory
from mapper_model.mapper_factory import MapperFactory
from common.helper import Helper
import asyncio
import time


class CopernicusOperation(Operation):
    def __init__(self):
        super().__init__()

    def perform_operation(self):
        try:
            print('CopernicusOperation in-progress');

            paths_to_parse = [
                # 'output.an.[time].grib',
                # 'output.fc.[time].grib',
                # 'cams_nrealtime.grib',
                'cams_nrealtime.fc.europe.grib',
            ];

            if paths_to_parse:
                for path in paths_to_parse:
                    folder_path, file_path = None, None

                    try:
                        if not Helper.is_path_parseable(path=path):
                            continue
                        start_time = time.time()
                        parser = ParserFactory.get_parser_for_path(path)
                        mapper = MapperFactory.get_mapper_for_path(path)
                        items = parser.parse(path=path, mapper=mapper)

                        # print('4. Parser {0}, Mapper: {1}'.format(parser, mapper))
                        # print('Path file {0}, items: {1}'.format(path, len(items)))

                        mapper.insert_items(items)
                        print("--- %s seconds ---" % (time.time() - start_time))
                        # mapper.update_file_parsed_flag(path)
                    except Exception as e:
                        print('Exception: {0}, {1}'.format(e, path))
                    finally:
                        if folder_path:
                            Helper.remove_directory(folder_path)

            print('CopernicusOperation complete');

        except Exception as e:
            print('Exception: {0}'.format(e))
