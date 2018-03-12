from datetime import datetime


class File:

    def __init__(self, filename=None, path=None, meta_information=None, priority=None):
        super().__init__()

        self.path = path
        self.filename = filename
        self.priority = priority

        if meta_information:
            modify_date = meta_information['modify']
            self.modify_date = datetime.strptime(modify_date, '%Y%m%d%H%M%S')

    def to_tuple(self):
        return self.filename, self.path, self.modify_date, self.priority

    def __str__(self)   :
        return '{0}, {1}'.format(self.modify_date, self.path)
