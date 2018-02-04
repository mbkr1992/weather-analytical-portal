
from constants import constants
from operation_model.txt_operation import TxtOperation


class OperationFactory:
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_operation(identifier):
        if identifier == constants.OPERATION_ZIP:
            return Helper()
        elif identifier == constants.OPERATION_TEXT:
            return TxtOperation()
