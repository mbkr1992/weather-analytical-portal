from postgis import Point


class Grib:

    def __init__(self, name, value, timestamp, latitude, longitude, unit, mars_type, mars_class, param_id, information, source):
        super().__init__()

        self.name = name
        self.value = value
        self.timestamp = timestamp
        self.position = Point(x=latitude, y=longitude, srid=4326),
        self.latitude = latitude      # int
        self.longitude = longitude  # int
        self.unit = unit  # str
        self.mars_type = mars_type
        self.mars_class = mars_class
        self.param_id = param_id
        self.information = information  # json
        self.source = source

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.name, self.value, self.timestamp)
