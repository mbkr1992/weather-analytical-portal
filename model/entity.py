
class Entity:

    def __init__(self, station_id=None, measurement_date=None):
        super().__init__()

        self.station_id = station_id
        self.measurement_date = measurement_date
        self.attribute_id = None
        self.value = None

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date, self.value)
