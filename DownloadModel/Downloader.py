from abc import ABC, abstractmethod


class Downloader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def download(self):
        pass