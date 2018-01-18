from abc import ABC, abstractmethod

class DownloaderFactory(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def getDownloader(self, identifier):
        pass