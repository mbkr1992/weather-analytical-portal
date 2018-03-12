class Grib:

    def __init__(self, name, value, timestamp, latitude, longitude, unit, mars_type, mars_class, param_id, information):
        super().__init__()

        self.name = name
        self.value = value
        self.timestamp = timestamp

        self.latitude = latitude      # int
        self.longitude = longitude  # int
        self.unit = unit  # str
        self.mars_type = mars_type
        self.mars_class = mars_class
        self.param_id = param_id
        self.information = information  # json

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.name, self.value, self.timestamp)
