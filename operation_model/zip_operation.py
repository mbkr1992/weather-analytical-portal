from download_model.downloader import Downloader
from parser_model.parser import Parser
from mapper_model.mapper import Mapper
from constants import constants
from common.helper import unzip, find
from operation_model.operation import Operation


class ZipOperation(Operation):
    def __init__(self):
        super().__init__()

    def perform_operation(self, path, downloader, parser, mapper):
        server_path, separator, filename = path.rpartition('/')
        downloader.download(filename)

        extracted_path = unzip(server_path + separator, filename)
        file_path = find('produkt_*.txt', extracted_path)

        items = parser.parse(file_path, mapper)
        return items
