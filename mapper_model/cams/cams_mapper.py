from mapper_model.mapper import Mapper
from model.grib import Grib
from datetime import datetime, timedelta
from psycopg2 import connect, extras
from postgis.psycopg import register
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler
import numpy as np
from postgis import Point


class CamsMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO data ' \
                            '(date, position, name, value, unit, mars_type, mars_class, param_id, source, position_str)' \
                            'VALUES %s' \
                            'ON CONFLICT (date, param_id, mars_type, mars_class) DO NOTHING '

    def map(self, grb=None):

        list_of_grib_objects = []

        values, lats, lons = grb.data(lat1=48, lat2=50, lon1=10, lon2=14)

        # lats, lons = grb.latlons()
        # values = get_value(grb, 'values', None)

        data_date = get_value(grb, 'dataDate', None)
        data_time = get_value(grb, 'dataTime', None)
        timestamp = datetime.strptime(str(data_date), '%Y%m%d') + timedelta(data_time)

        name = get_value(grb, 'name', None)
        unit = get_value(grb, 'units', None)

        mars_type = get_value(grb, 'marsType', None)
        mars_class = get_value(grb, 'marsClass', None)
        param_id = get_value(grb, 'paramId', None)
        maximum = get_value(grb, 'maximum', None)
        minimum = get_value(grb, 'minimum', None)
        average = get_value(grb, 'average', None)

        for (x, y), value in np.ndenumerate(values):
            latitude = round(lats[x, y], 10)
            longitude = round(lons[x, y], 10)
            grb_obj = Grib(name=name, value=round(value, 10), timestamp=timestamp, latitude=latitude, longitude=longitude,
                           unit=unit, mars_type=mars_type, mars_class=mars_class, param_id=param_id, information={
                    "max": maximum,
                    "min": minimum,
                })
            list_of_grib_objects.append(grb_obj)

        return list_of_grib_objects

    @staticmethod
    def to_tuple(item):
        return (
            item.timestamp,
            Point(x=item.longitude, y=item.latitude, srid=4326),
            item.name,
            extras.Json(item.value),
            item.unit,
            item.mars_type,
            item.mars_class,
            item.param_id,
            item.source,
            '({}, {})'.format(item.latitude, item.longitude),
        )

    def insert_items(self, items, position=None):
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


def get_value(grb, key, default):
    if not grb.valid_key(key):
        return default

    if grb[key] == 9999:
        return default

    if isinstance(grb[key], float):
        return round(grb[key], 6)

    return grb[key]