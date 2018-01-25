from abc import ABC, abstractmethod


class Mapper(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def map(self, item):
        pass

