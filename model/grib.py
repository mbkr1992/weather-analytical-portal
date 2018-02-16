class Grib:

    def __init__(self, name, value, timestamp, latitude, longitude, unit, information):
        super().__init__()

        self.name = name
        self.value = value
        self.timestamp = timestamp

        self.latitude = latitude      # int
        self.longitude = longitude  # int
        self.unit = unit  # str
        self.information = information  # json

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.name, self.value, self.timestamp)
