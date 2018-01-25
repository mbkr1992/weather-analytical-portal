from constants import constants
from operation_model.zip_operation import ZipOperation


class OperationFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_operation(identifier):
        if identifier == constants.OPERATION_ZIP:
            return ZipOperation()
