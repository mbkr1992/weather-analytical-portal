from mapper_model.water_equiv.water_equiv_mapper import WaterEquivMapper
from model.water_equiv import WaterEquiv
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class WaterEquivDailyMapper(WaterEquivMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

    def map(self, item={}):
        water_equiv = WaterEquiv()
        water_equiv.station_id = item['STATIONS_ID']
        water_equiv.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        water_equiv.measurement_category = 'daily'

        water_equiv.qn_6 = item.get('QN_6', None)
        if water_equiv.qn_6 == '-999':
            water_equiv.qn_6 = None

        water_equiv.ash_6 = item.get('ASH_6', None)
        if water_equiv.ash_6 == '-999':
            water_equiv.ash_6 = None

        water_equiv.sh_tag = item.get('SH_tag', None)
        if water_equiv.sh_tag == '-999':
            water_equiv.sh_tag = None

        water_equiv.wash_6 = item.get('WASH_6', None)
        if water_equiv.wash_6 == '-999':
            water_equiv.wash_6 = None

        water_equiv.waas_6 = item.get('WAAS_6', None)
        if water_equiv.waas_6 == '-999':
            water_equiv.waas_6 = None

        return water_equiv

    def insert_items(self, items):
        super().insert_items(items)

    def update_file_parsed_flag(self, path):
        super().update_file_parsed_flag(path)
