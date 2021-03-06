from abc import ABC, abstractmethod


class Downloader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def download(self, path):
        pass

    @abstractmethod
    def download_meta(self, path):
        pass