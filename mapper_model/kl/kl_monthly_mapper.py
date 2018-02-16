from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler

class KlMonthlyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        kl = Kl()
        kl.station_id = item['STATIONS_ID']
        kl.measurement_date = datetime.strptime(item['MESS_DATUM_BEGINN'], '%Y%m%d')
        kl.measurement_category = 'monthly'

        kl.information = list()

        # qn_4 = item.get('QN_4', None)
        # if self.is_valid(qn_4):
        #     kl.information.append(
        #         dict(
        #             value=qn_4,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        mo_n = item.get('MO_N', None)
        if self.is_valid(mo_n):
            kl.information.append(
                dict(
                    name='MO_N',
                    value=mo_n,
                    unit='1/8',
                    description='monthly mean of cloud cover',
                )
            )

        mo_tt = item.get('MO_TT', None)
        if self.is_valid(mo_tt):
            kl.information.append(
                dict(
                    name='MO_TT',
                    value=mo_tt,
                    unit='°C',
                    description='monthly mean of daily temperature means in 2m',
                )
            )

        mo_tx = item.get('MO_TX', None)
        if self.is_valid(mo_tx):
            kl.information.append(
                dict(
                    name='MO_TX',
                    value=mo_tx,
                    unit='°C',
                    description='monthly mean of daily temperature maxima in 2m height',
                )
            )

        mo_tn = item.get('MO_TN', None)
        if self.is_valid(mo_tn):
            kl.information.append(
                dict(
                    name='MO_TN',
                    value=mo_tn,
                    unit=NOT_AVAILABLE,
                    description='monthly mean of daily temperature minima in 2m height',
                )
            )

        mo_sd_s = item.get('MO_SD_S', None)
        if self.is_valid(mo_sd_s):
            kl.information.append(
                dict(
                    name='MO_SD_S',
                    value=mo_sd_s,
                    unit='h',
                    description='monthly sum of sunshine duration',
                )
            )

        mo_fk = item.get('MO_FK', None)
        if self.is_valid(mo_fk):
            kl.information.append(
                dict(
                    name='MO_FK',
                    value=mo_fk,
                    unit='Bft',
                    description='monthly mean of daily wind speed',
                )
            )

        mx_tx = item.get('MX_TX', None)
        if self.is_valid(mx_tx):
            kl.information.append(
                dict(
                    name='MX_TX',
                    value=mx_tx,
                    unit='°C',
                    description='monthly maximum of daily temperature maxima in 2m height',
                )
            )

        mx_fx = item.get('MX_FX', None)
        if self.is_valid(mx_fx):
            kl.information.append(
                dict(
                    name='MX_FX',
                    value=mx_fx,
                    unit='m/sec',
                    description='monthly maximum of daily wind speed',
                )
            )

        mx_tn = item.get('MX_TN', None)
        if self.is_valid(mx_tn):
            kl.information.append(
                dict(
                    name='MX_TN',
                    value=mx_tn,
                    unit='°C',
                    description='monthly minimum of daily temperature minima in 2m height',
                )
            )

        # qn_6 = item.get('QN_6', None)
        # if self.is_valid(qn_6):
        #     kl.information.append(
        #         dict(
        #             name='QN_6',
        #             value=qn_6,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        mo_rr = item.get('MO_RR', None)
        if self.is_valid(mo_rr):
            kl.information.append(
                dict(
                    name='MO_RR',
                    value='monthly sum of precipitation height',
                    unit='mm',
                    description=NOT_AVAILABLE,
                )
            )

        mx_rs = item.get('MX_RS', None)
        if self.is_valid(mx_rs):
            kl.information.append(
                dict(
                    name='MX_RS',
                    value=mx_rs,
                    unit='mm',
                    description='monthly maximum of daily',
                )
            )

        return kl


    @staticmethod
    def is_valid(value):
        return value and value != '999'

    @staticmethod
    def to_tuple(item: Kl):
        return (item.station_id,
                item.measurement_date,
                item.measurement_category,
                extras.Json(item.information))

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
