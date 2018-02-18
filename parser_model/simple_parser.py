from parser_model.parser import Parser
import re


class SimpleParser(Parser):
    def __init__(self):
        super().__init__()

    def parse(self, path, mapper):
        with open(path, encoding='ISO-8859-1') as f:
            list_of_items = []
            try:
                # Headings Station_id etc
                keys = clean_txt(f.readline()).split(';')

                # Removing eor column found in  many files
                last_column = keys[-1]
                if last_column == 'eor':
                    keys.pop(len(keys) - 1)

                # Actual values 00183 etc

                for line in f:
                    values = clean_txt(line).split(';')
                    item = {}
                    for i in range(0, len(keys)):

                        # For cases where data in a row is incomplete
                        if i < len(values) - 1:
                            item[keys[i]] = values[i]
                    list_of_items.extend(mapper.map(item))
            except Exception as e:
                print('Parsing error {0}'.format(e, path))
            finally:
                f.close()
            return list_of_items


def clean_txt(txt):
    without_spaces = remove_spaces(txt)
    if without_spaces.endswith('eor') or without_spaces.endswith('eor;'):
        return re.sub(r'(eor);*', '', without_spaces)
    return without_spaces


def remove_spaces(txt):
    return re.sub(r'\s+', '', txt)