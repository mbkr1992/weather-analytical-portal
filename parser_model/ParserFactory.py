from abc import ABC, abstractmethod

class ParserFactory(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def getParser(self, identifier):
        pass