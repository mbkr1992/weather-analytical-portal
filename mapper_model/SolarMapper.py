from mapper_model.Mapper import Mapper
from model.Solar import Solar


class SolarMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item):
        return Solar(item)