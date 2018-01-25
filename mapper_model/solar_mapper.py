from mapper_model.mapper import Mapper
from model.solar import Solar
from datetime import datetime


class SolarMapper(Mapper):

    def __init__(self, solar=None):
        super().__init__()
        self.solar = solar

    def map(self, item):
        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H:%M')
        solar.qn = item['QN_592']
        solar.atmo_radiation = item['ATMO_LBERG']
        solar.fd_radiation = item['FD_LBERG']
        solar.fg_radiation = item['FG_LBERG']
        solar.sd_radiation = item['SD_LBERG']
        solar.zenith = item['ZENIT']
        solar.measurement_date_local = datetime.strptime(item['MESS_DATUM_WOZ'], '%Y%m%d%H:%M')

        return solar
