from abc import ABC, abstractmethod


class Parser(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def parse(self):
        pass