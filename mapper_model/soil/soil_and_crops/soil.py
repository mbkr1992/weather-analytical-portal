from mapper_model.mapper import Mapper
from model.soil_temperature import SoilTemperature
from datetime import datetime
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler


class SoilDailyMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION

        self.insert_query = db_handler.query_insert_station_data

        self.update_query = db_handler.query_update_file_is_parsed_flag

    def map(self, item={}):
        soil_temperature = SoilTemperature()
        soil_temperature.station_id = item['Stationsindex']
        soil_temperature.measurement_date = datetime.strptime(item['Datum'], '%Y%m%d')
        soil_temperature.measurement_category = 'daily'

        soil_temperature.information = list()

        # qn_2 = item.get('QN_2', None)
        # if self.is_valid(qn_2):
        #     soil_temperature.information.append(
        #         dict(
        #             name='QN_2',
        #             value=qn_2,
        #             unit=NOT_AVAILABLE,
        #             description='quality level of next columns',
        #         )
        #     )

        vgsl = item.get('VGSL', None)
        if self.is_valid(vgsl):
            soil_temperature.information.append(
                dict(
                    name='VGSL',
                    value=vgsl,
                    unit='mm',
                    description='real evapotranspiration over grass and sandy loam (AMBAV)',
                )
            )

        vpgb = item.get('VPGB', None)
        if self.is_valid(vpgb):
            soil_temperature.information.append(
                dict(
                    name='VPGB',
                    value=vpgb,
                    unit='mm',
                    description='potential evapotranspiration over grass (AMBAV)',
                )
            )

        vpgh = item.get('VPGH', None)
        if self.is_valid(vpgh):
            soil_temperature.information.append(
                dict(
                    name='VPGH',
                    value=vpgh,
                    unit='mm',
                    description='potential evaporation over grass (Haude)',
                )
            )

        ts05 = item.get('TS05', None)
        if self.is_valid(ts05):
            soil_temperature.information.append(
                dict(
                    name='TS05',
                    value=ts05,
                    unit='°C',
                    description='mean daily soil temperature in 5 cm depth for uncovered typical soil in 5 cm depth '
                                'mean daily soil',
                )
            )

        ts10 = item.get('TS10', None)
        if self.is_valid(ts10):
            soil_temperature.information.append(
                dict(
                    name='TS10',
                    value=ts10,
                    unit='°C',
                    description='mean daily soil temperature in 5 cm depth for uncovered typical soil in 10 cm depth '
                                'mean daily soil',
                )
            )

        ts20 = item.get('TS20', None)
        if self.is_valid(ts20):
            soil_temperature.information.append(
                dict(
                    name='TS20',
                    value=ts20,
                    unit='°C',
                    description='mean daily soil temperature in 5 cm depth for uncovered typical soil in 20 cm depth '
                                'mean daily soil',
                )
            )

        ts50 = item.get('TS50', None)
        if self.is_valid(ts50):
            soil_temperature.information.append(
                dict(
                    name='TS50',
                    value=ts50,
                    unit='°C',
                    description='mean daily soil temperature in 5 cm depth for uncovered typical soil in 50 cm depth '
                                'mean daily soil',
                )
            )

        ts100 = item.get('TS100', None)
        if self.is_valid(ts100):
            soil_temperature.information.append(
                dict(
                    name='TS100',
                    value=ts100,
                    unit='°C',
                    description='mean daily soil temperature in 5 cm depth for uncovered typical soil in 1 m depth '
                                'mean daily soil',
                )
            )
        zfumi = item.get('ZFUMI', None)
        if self.is_valid(zfumi):
            soil_temperature.information.append(
                dict(
                    name='ZFUMI',
                    value=zfumi,
                    unit='cm',
                    description='frost depth at midday for uncovered soil',
                )
            )
        bf10 = item.get('BF10', None)
        if self.is_valid(bf10):
            soil_temperature.information.append(
                dict(
                    name='BF10',
                    value=bf10,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 0 and 10 cm depth in % plant '
                                'usable water',
                )
            )

        bf20 = item.get('BF20', None)
        if self.is_valid(bf20):
            soil_temperature.information.append(
                dict(
                    name='BF20',
                    value=bf20,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 10 and 20 cm depth in % plant '
                                'usable water',
                )
            )

        bf30 = item.get('BF30', None)
        if self.is_valid(bf30):
            soil_temperature.information.append(
                dict(
                    name='BF30',
                    value=bf30,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 20 and 30 cm depth in % plant '
                                'usable water',
                )
            )

        bf40 = item.get('BF40', None)
        if self.is_valid(bf40):
            soil_temperature.information.append(
                dict(
                    name='BF40',
                    value=bf40,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 30 and 40 cm depth in % plant '
                                'usable water',
                )
            )

        bf50 = item.get('BF50', None)
        if self.is_valid(bf50):
            soil_temperature.information.append(
                dict(
                    name='BF50',
                    value=bf50,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 40 and 50 cm depth in % plant '
                                'usable water',
                )
            )

        bf60 = item.get('BF60', None)
        if self.is_valid(bf60):
            soil_temperature.information.append(
                dict(
                    name='BF60',
                    value=bf60,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam between 50 and 60 cm depth in % plant '
                                'usable water',
                )
            )

        bfgsl = item.get('BFGSL', None)
        if self.is_valid(bfgsl):
            soil_temperature.information.append(
                dict(
                    name='BFGSL',
                    value=bfgsl,
                    unit='%nFK',
                    description='soil moisture under grass and sandy loam up to 60 cm depth',
                )
            )

        bfgls = item.get('BFGLS', None)
        if self.is_valid(bfgls):
            soil_temperature.information.append(
                dict(
                    name='BFGLS',
                    value=bfgls,
                    unit='%nFK',
                    description='soil moisture under grass and loamy sand up to 60 cm depth',
                )
            )

        return soil_temperature

    @staticmethod
    def is_valid(value):
        return value and value != '999'

    @staticmethod
    def to_tuple(item):
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