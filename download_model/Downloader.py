from abc import ABC, abstractmethod


class Downloader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def download(self, server, username, password, server_path, filename):
        pass