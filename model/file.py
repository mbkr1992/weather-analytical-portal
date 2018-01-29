from datetime import datetime


class File:

    def __init__(self, path=None, meta_information=None):
        super().__init__()

        self.path = path

        if meta_information:
            modify_date = meta_information['modify']
            self.modify_date = datetime.strptime(modify_date, '%Y%m%d%H%M%S')

    def to_tuple(self):
        return self.path, self.modify_date

    def __str__(self):
        return '{0}, {1}'.format(self.modify_date, self.path)
