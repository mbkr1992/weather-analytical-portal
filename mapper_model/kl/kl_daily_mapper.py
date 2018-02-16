from mapper_model.mapper import Mapper
from model.kl import Kl
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class KlDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        kl = Kl()
        kl.station_id = item['STATIONS_ID']
        kl.measurement_date = datetime.strptime(item['MESS_DATUM'], '%Y%m%d')
        kl.measurement_category = 'daily'

        kl.information = list()

        # qn_3 = item.get('QN_3', None)
        # if self.is_valid(qn_3):
        #     kl.information.append(
        #         dict(
        #             value=qn_3,
        #             unit=NOT_AVAILABLE,
        #         )
        #     )
            
        fx = item.get('FX', None)
        if self.is_valid(fx):
            kl.information.append(
                dict(
                    name='FX',
                    value=fx,
                    unit='m/s',
                    description='daily maximum of wind gust',
                )
            )

        fm = item.get('FM', None)
        if self.is_valid(fm):
            kl.information.append(
                dict(
                    name='FM',
                    value=fm,
                    unit='m/s',
                    description='daily mean of wind velocity',
                )
            )

        # qn_4 = item.get('QN_4', None)
        # if self.is_valid(qn_4):
        #     kl.information.append(
        #         dict(
        #             value=qn_4,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        rsk = item.get('RSK', None)
        if self.is_valid(rsk):
            kl.information.append(
                dict(
                    name='RSK',
                    value=rsk,
                    unit='mm',
                    description='daily precipitation height',
                )
            )

        rskf = item.get('RSKF', None)
        if self.is_valid(rskf):
            kl.information.append(
                dict(
                    name='RSKF',
                    value=rskf,
                    unit=NOT_AVAILABLE,
                    description='precipitation form',
                )
            )

        sdk = item.get('SDK', None)
        if self.is_valid(sdk):
            kl.information.append(
                dict(
                    name='SDK',
                    value=sdk,
                    unit='h',
                    description='daily sunshine duration',
                )
            )

        shk_tag = item.get('SHK_TAG', None)
        if self.is_valid(shk_tag):
            kl.information.append(
                dict(
                    name='SHK_TAG',
                    value=shk_tag,
                    unit='cm',
                    description='daily snow depth',
                )
            )

        nm = item.get('NM', None)
        if self.is_valid(nm):
            kl.information.append(
                dict(
                    name='NM',
                    value=nm,
                    unit='1/8',
                    description='daily mean of cloud cover',
                )
            )

        vpm = item.get('VPM', None)
        if self.is_valid(vpm):
            kl.information.append(
                dict(
                    name='VPM',
                    value=vpm,
                    unit='hPa',
                    description='daily mean of vapor pressure',
                )
            )

        pm = item.get('PM', None)
        if self.is_valid(pm):
            kl.information.append(
                dict(
                    name='PM',
                    value=pm,
                    unit='hPa',
                    description='daily mean of pressure',
                )
            )

        tmk = item.get('TMK', None)
        if self.is_valid(tmk):
            kl.information.append(
                dict(
                    name='TMK',
                    value=tmk,
                    unit='°C',
                    description='daily mean of temperature',
                )
            )

        upm = item.get('UPM', None)
        if self.is_valid(upm):
            kl.information.append(
                dict(
                    name='UPM',
                    value=upm,
                    unit='%',
                    description='daily mean of relative humidity',
                )
            )

        txk = item.get('TXK', None)
        if self.is_valid(txk):
            kl.information.append(
                dict(
                    name='TXK',
                    value=txk,
                    unit='°C',
                    description='daily maximum of temperature at °C 2m height',
                )
            )

        tnk = item.get('TNK', None)
        if self.is_valid(tnk):
            kl.information.append(
                dict(
                    name='TNK',
                    value=tnk,
                    unit='°C',
                    description='daily minimum of temperature at 2m height',
                )
            )

        tgk = item.get('TGK', None)
        if self.is_valid(tgk):
            kl.information.append(
                dict(
                    name='TGK',
                    value=tgk,
                    unit='°C',
                    description='daily minimum of air temperature at 5cm above ground',
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

