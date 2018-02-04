from mapper_model.cloudiness.cloudiness_mapper import CloudinessMapper
from model.cloudiness import Cloudiness
from datetime import datetime
from constants.constants import DATABASE_CONNECTION


class CloudinessHourlyMapper(CloudinessMapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = None
        self.select_query = None

    def map(self, item={}):
        cloudiness = Cloudiness()
        cloudiness.station_id = item['STATIONS_ID']
        cloudiness.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d%H')
        cloudiness.measurement_category = 'hourly'

        cloudiness.qn_8 = item.get('QN_8', None)
        if cloudiness.qn_8 == '-999':
            cloudiness.qn_8 = None

        cloudiness.v_n_i = item.get('V_N_I', None)
        if cloudiness.v_n_i == '-999':
            cloudiness.v_n_i = None

        cloudiness.v_n = item.get('V_N', None)
        if cloudiness.v_n == '-999':
            cloudiness.v_n = None

        return cloudiness

    def insert_items(self, items):
        super(CloudinessHourlyMapper, self).insert_items(items)

    def update_file_parsed_flag(self, path):
        super(CloudinessHourlyMapper, self).update_file_parsed_flag(path)
