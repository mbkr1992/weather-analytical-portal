from mapper_model.water_equiv.water_equiv_mapper import WaterEquivMapper
from model.water_equiv import WaterEquiv
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION


class WaterEquivDailyMapper(WaterEquivMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data_hub (' \
                            'station_id, measurement_date, measurement_category, ' \
                            'water_equiv_qn_6, water_equiv_ash_6, ' \
                            'water_equiv_sh_tag, water_equiv_wash_6, water_equiv_waas_6)' \
                            'VALUES %s' \
                            'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

        self.update_query = 'UPDATE file_meta SET is_parsed =(%S) WHERE path =(%S);'

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

    @staticmethod
    def to_tuple(item):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                item.qn_6,
                item.ash_6,
                item.sh_tag,
                item.wash_6,
                item.waas_6)

    def insert_items(self, items):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = True, path
                curs.execute(self.update_query, data)
