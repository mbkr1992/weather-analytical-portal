from mapper_model.mapper import Mapper
from model.grib import Grib
from model.grib_database_adapter_model import GDBAdapter
from datetime import datetime, timedelta
from psycopg2 import connect, extras
from postgis.psycopg import register
from psycopg2.extensions import register_adapter
from constants.constants import DATABASE_CONNECTION, NOT_AVAILABLE
from database_model import db_handler
import numpy as np
from postgis import Point
from collections import defaultdict


class CamsMapper(Mapper):

    def __init__(self):
        super().__init__()
        self.dbc = DATABASE_CONNECTION
        self.insert_query = 'INSERT INTO satellite_data (date, position, position_text, information, meta_information)' \
                            'VALUES %s' \
                            # 'ON CONFLICT (measurement_date, measurement_category, station_id) DO NOTHING '

    def map(self, grbs):
        # anobj = Grib(name='Hello', value=1234, timestamp=None, latitude=20, longitude=123,
        #      unit='C', mars_type='a', mars_class='b', param_id=123,
        #      max=111, min=2)
        # print('Coplte', anobj.to_json())
        # return {}
        grbs.seek(0)

        dict_of_grib_objects = defaultdict(list)
        for grb in grbs:
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
                latitude = lats[x, y]
                longitude = lons[x, y]

                information = {
                    "name": name, "unit": unit,
                }

                meta_information = {
                    "type": mars_type, "class": mars_class, "param_id": param_id, "maximum": maximum, "minimum": minimum, "average": average,
                }

                dict_of_grib_objects[timestamp, latitude, longitude].append({
                    "information": information,
                    "meta_information": meta_information,
                })

        return [to_db(dict_of_grib_objects, key) for key in dict_of_grib_objects.keys()]

    @staticmethod
    def to_tuple(item):
        return (item.date,
                Point(x=item.latitude, y=item.longitude, srid=4326),
                'Point({0}, {1})'.format(item.latitude, item.longitude),
                extras.Json(item.information),
                extras.Json(item.meta_information),
                )

    def insert_items(self, items):
        with connect(self.dbc) as conn:
            register(connection=conn)
            with conn.cursor() as curs:
                data = [self.to_tuple(item) for item in items]
                extras.execute_values(curs, self.insert_query, data, template=None, page_size=100)

    def update_file_parsed_flag(self, path):
        pass
        # with connect(self.dbc) as conn:
        #     register(connection=conn)
        #     with conn.cursor() as curs:
        #         data = True, path
        #         curs.execute(self.update_query, data)


def get_value(grb, key, default):
    if not grb.valid_key(key):
        return default

    if grb[key] == 9999:
        return default

    if isinstance(grb[key], float):
        return round(grb[key], 6)

    return grb[key]


def to_db(dict_grbs, key):
    date, lat, lon = key
    information, meta_information = zip(*[(value['information'], value['meta_information']) for value in dict_grbs[key]])
    return GDBAdapter(date=date, latitude=lat, longitude=lon, information=information, meta_information=meta_information)
