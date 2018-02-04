from mapper_model.solar_mappers.solar_mapper import SolarMapper
from model.solar import Solar
from datetime import datetime


class SolarHourlyMapper(SolarMapper):

    def __init__(self):
        super().__init__()

    def map(self, item={}):
        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H:%M')
        solar.measurement_category = 'daily'
        solar.qn = item['QN_592']

        solar.atmo_radiation = item.get('ATMO_LBERG', None)
        if solar.atmo_radiation == '-999':
            solar.atmo_radiation = None

        solar.fd_radiation = item.get('FD_LBERG', None)
        if solar.fd_radiation == '-999':
            solar.fd_radiation = None

        solar.fg_radiation = item.get('FG_LBERG', None)
        if solar.fg_radiation == '-999':
            solar.fg_radiation = None

        solar.sd_radiation = item.get('SD_LBERG', None)
        if solar.sd_radiation == '-999':
            solar.sd_radiation = None

        solar.zenith = item.get('ZENIT', None)
        solar.measurement_date_local = datetime.strptime(item['MESS_DATUM_WOZ'], '%Y%m%d%H:%M')

        return solar

    def insert_items(self, items):
        super(SolarHourlyMapper, self).insert_items(items)

    def update_file_parsed_flag(self, path):
        super(SolarHourlyMapper, self).update_file_parsed_flag(path)
