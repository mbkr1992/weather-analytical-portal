from operation_model.operation import Operation
from database_model.db_handler import update_file_download_flag, insert_files, select_non_downloaded_files_satellite, select_non_parsed_files_satellite
from constants import constants
from parser_model.parser_factory import ParserFactory
from mapper_model.mapper_factory import MapperFactory
from common.helper import Helper
import asyncio
from datetime import datetime
from calendar import monthrange
from ecmwfapi import ECMWFDataServer
from model.file import File

server = ECMWFDataServer()


class CopernicusOperation(Operation):
    def __init__(self):
        super().__init__()

    def perform_operation(self):
        try:
            print('CopernicusOperation in-progress');

            # Generate all the file names, populate into database, if already not there,
            files_to_insert = []
            files_to_insert.extend([to_file(date, 'an') for date in list_of_dates()])
            files_to_insert.extend([to_file(date, 'fc') for date in list_of_dates()])
            insert_files(files_to_insert)

            # Fetch all the file objects where is_downloaded = false
            # Download a file, update is_downloaded = true
            files_to_download = select_non_downloaded_files_satellite()
            for file in files_to_download:
                if download_file(file):
                    update_file_download_flag(file.path)

            # Fetch all the file objects where is_parsed = false
            # Insert all the items, update is_parsed = true
            paths_to_parse = select_non_parsed_files_satellite()

            if paths_to_parse:
                for path in paths_to_parse:
                    folder_path, file_path = None, None

                    try:
                        if not Helper.is_path_parseable(path=path):
                            continue

                        parser = ParserFactory.get_parser_for_path(path)
                        mapper = MapperFactory.get_mapper_for_path(path)
                        items = parser.parse(path=path, mapper=mapper)

                        # print('4. Parser {0}, Mapper: {1}'.format(parser, mapper))
                        # print('Path file {0}, items: {1}'.format(path, len(items)))

                        # mapper.insert_items(items)
                        # mapper.update_file_parsed_flag(path)
                    except Exception as e:
                        print('Exception: {0}, {1}'.format(e, path))
                    finally:
                        if folder_path:
                            Helper.remove_directory(folder_path)

            print('CopernicusOperation complete');

        except Exception as e:
            print('Exception: {0}'.format(e))


def list_of_dates():
    finish_date = datetime.now()
    s_year, s_month, s_day = 2016, 6, 1
    e_year, e_month, e_day = finish_date.year, finish_date.month, finish_date.day

    dates = []
    while True:
        from_date = datetime(s_year, s_month, s_day)

        if s_year == e_year and s_month == e_month:
            break

        dates.append(from_date)

        if s_month == 12:
            s_month = 1
            s_year += 1
        else:
            s_month += 1

    return dates


def download_file(file):
    date = file.modify_date
    path = file.path

    _, passing_month_end = monthrange(date.year, date.month)
    to_date = datetime(date.year, date.month, passing_month_end)

    formatted_date = '{}/to/{}'.format(date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

    is_type_an = True if path.endswith('.an.grib') else False

    try:
        if is_type_an:
            server.retrieve({
                "class": "mc",
                "dataset": "cams_nrealtime",
                "date": formatted_date,
                "expver": "0001",
                "levtype": "sfc",
                "param": "4.218/6.218/13.218/16.218/27.218/30.218/34.128/45.218/47.218/52.210/53.210/125.210/126.210/127.210/128.210/129.128/137.128/151.128/164.128/165.128/166.128/167.128/168.128/172.128/174.128/186.128/187.128/188.128/206.210",
                "step": "0",
                "stream": "oper",
                "time": "00:00:00",
                "type": "an",
                "target": path,
            })
        else:
            server.retrieve({
                "class": "mc",
                "dataset": "cams_nrealtime",
                "date": formatted_date,
                "expver": "0001",
                "levtype": "sfc",
                "param": "2.214/3.214/4.218/6.218/13.218/16.218/27.218/30.218/34.128/45.218/47.218/52.210/53.210/72.210/73.210/74.210/125.210/126.210/127.210/128.210/137.128/142.128/143.128/144.128/151.128/164.128/165.128/166.128/167.128/168.128/172.128/186.128/187.128/188.128/206.210/207.210/208.210/209.210/210.210/211.210/212.210/213.210/214.210/215.210/216.210/243.128",
                "step": "0/3/6/9/12/15/18/21",
                "stream": "oper",
                "time": "00:00:00",
                "type": "fc",
                "target": path,
            })

        return True
    except Exception as e:
        return False


def to_file(date, mars_class):
    filename = date.strftime('%Y%m')
    file_path = "downloads/cams.{}.{}.grib".format(filename, mars_class)
    return File(filename=filename, path=file_path, priority=1, modify_date=date)
