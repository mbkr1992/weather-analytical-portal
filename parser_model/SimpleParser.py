from parser_model.Parser import Parser
from common.Constants import DOWNLOAD_FOLDER


class SimpleParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path_file, mapper):
        print('Parsing file: ' + path_file, mapper);

        temporary_path = path_file

        with open(temporary_path, encoding='ISO-8859-1') as f:
            list_of_items = []

            # Headings Station_id etc
            keys = f.readline().split(';')

            # Actual values 00183 etc
            for line in f:
                values = line.split(';')
                item = {}
                for i in range(0, len(keys)):
                    item[keys[i].strip()] = values[i].strip()
                print(item)
                list_of_items.append(mapper.map(item))
            f.close()
            return list_of_items
