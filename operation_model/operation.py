from abc import ABC, abstractmethod


class Operation(ABC):
    def __init__(self):
        super().__init__()

        self.server = 'ftp-cdc.dwd.de'
        self.username = 'anonymous'
        self.password = 'anonymous'

    @abstractmethod
    def perform_operation(self, path):
        pass