from abc import ABC, abstractmethod


class Mapper(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def map(self, item):
        pass

    @abstractmethod
    def insert_items(self, items):
        pass

    @abstractmethod
    def update_file_parsed_flag(self, path):
        pass