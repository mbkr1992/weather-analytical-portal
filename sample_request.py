from operation_model.german_weather_operation import GermanWeatherOperation
from operation_model.copernicus_operation import CopernicusOperation


def perform_async_operation():
    GermanWeatherOperation().perform_operation()
    # CopernicusOperation().perform_operation()
    pass


def main():
    perform_async_operation()

main()

