from parser_model.parser import Parser
import pygrib
import numpy as np
from common.helper import Helper


class GribParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path, mapper):
        grbs = pygrib.open(path)
        list_of_items = []

        try:
            grbs.seek(0)

            for grb in grbs:
                # grb = grbs.read(1)[0]
                list_of_items.extend(mapper.map(grb))
        except Exception as e:
            print('Parsing error {0}'.format(e, path))
        finally:
            grbs.close()
        return list_of_items
