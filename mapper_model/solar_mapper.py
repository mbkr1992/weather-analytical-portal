from mapper_model.mapper import Mapper
from model.solar import Solar
from model.entity import Entity
from constants.solar_enum import SolarEnum


class SolarMapper(Mapper):

    def __init__(self, solar=None):
        super().__init__()
        self.solar = solar

    def map(self, item):
        solar = Solar()
        solar.station_id = item['STATIONS_ID']
        solar.measurement_date = item['MESS_DATUM']
        solar.qn = item['QN_592']
        solar.atmo_radiation = item['ATMO_LBERG']
        solar.fd_radiation = item['FD_LBERG']
        solar.fg_radiation = item['FG_LBERG']
        solar.sd_radiation = item['SD_LBERG']
        solar.zenith = item['ZENIT']
        solar.measurement_date_solar = item['MESS_DATUM_WOZ']
        return solar

    def to_entities(self) -> [Entity]:
        entities = list()
        entities.append(self.qn_to_entity())
        entities.append(self.atmo_to_entity())
        entities.append(self.fd_to_entity())
        entities.append(self.fg_to_entity())
        entities.append(self.sd_to_entity())
        entities.append(self.zenith_to_entity())
        entities.append(self.solar_date_to_entity())
        return entities

    def qn_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.qn
        entity.value = self.solar.qn
        return entity

    def atmo_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.atmo_radiation
        entity.value = self.solar.atmo_radiation
        return entity

    def fd_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.fd_radiation
        entity.value = self.solar.fd_radiation
        return entity

    def fg_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.fg_radiation
        entity.value = self.solar.fg_radiation
        return entity

    def sd_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.sd_radiation
        entity.value = self.solar.sd_radiation
        return entity

    def zenith_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.zenith
        entity.value = self.solar.zenith
        return entity

    def solar_date_to_entity(self):
        entity = Entity(station_id=self.solar.station_id, measurement_date=self.solar.measurement_date)
        entity.attribute_id = SolarEnum.measurement_date_solar
        entity.value = self.solar.measurement_date_solar
        return entity