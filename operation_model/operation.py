from abc import ABC, abstractmethod
from download_model.downloader import Downloader
from parser_model.parser import Parser
from mapper_model.mapper import Mapper


class Operation(ABC):
    def __init__(self):
        super().__init__()

        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    @abstractmethod
    def perform_operation(self, downloader: Downloader, parser: Parser, mapper: Mapper):
        pass