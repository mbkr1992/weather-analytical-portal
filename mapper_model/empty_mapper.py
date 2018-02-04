from mapper_model.mapper import Mapper
from model.station import Station


class EmptyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item):
        return None

    def insert_items(self, items):
        pass

    def update_file_parsed_flag(self, path):
        pass