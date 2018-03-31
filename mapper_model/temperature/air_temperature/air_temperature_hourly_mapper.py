from mapper_model.mapper import Mapper
from model.air_temperature import AirTemperature
from datetime import datetime

class AirTemperatureHourlyMapper(Mapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):

        list_of_items = []

        station_id = item['STATIONS_ID']
        date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        interval = 'hourly'

        list_of_items.append(create_tt_tu(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        list_of_items.append(create_rf_tu(
            item=item,
            sid=station_id,
            date=date,
            interval=interval,
        ))

        return list_of_items


def create_tt_tu(sid, date, interval, item):
    qn = item.get('QN_9', None)
    code = 'TT_TU'
    name = 'Air temperature in 2m height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='°C',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def create_rf_tu(sid, date, interval, item):
    qn = item.get('QN_9', None)
    code = 'RF_TU'
    name = 'Relative humidity at 2m height'
    value = get_value(item, code, None)
    return AirTemperature(station_id=sid, date=date,
                          interval=interval, name=name, unit='%',
                          value=value,
                          information={
                              "QN": qn,
                              "code": code,
                          })


def get_value(item, key, default):
    if key not in item:
        return default

    if item[key] == '-999':
        return default

    return item[key]
