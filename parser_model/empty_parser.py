from parser_model.parser import Parser


class EmptyParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path, mapper):
        return []
