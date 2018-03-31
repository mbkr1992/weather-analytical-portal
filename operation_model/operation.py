from abc import ABC, abstractmethod


class Operation(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def perform_operation(self, path):
        pass