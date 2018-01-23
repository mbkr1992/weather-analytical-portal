from mapper_model.mapper import Mapper
from model.solar import Solar


class SolarMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item):
        return Solar(item)