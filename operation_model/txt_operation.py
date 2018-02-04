
from operation_model.operation import Operation


class TxtOperation(Operation):
    def __init__(self):
        super().__init__()

    def perform_operation(self, path, downloader, parser, mapper):
        server_path, separator, filename = path.rpartition('/')
        downloader.download(self.server, self.username, self.password, server_path + separator, filename)

        items = parser.parse(path, mapper)
        return items
