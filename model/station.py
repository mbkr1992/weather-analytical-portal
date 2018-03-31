from postgis import Point


class Station:

    def __init__(self, longitude=None, latitude=None):
        super().__init__()

        self.id = id
        self.name = None
        self.height = None
        self.latitude = latitude
        self.longitude = longitude
        if self.latitude and self.longitude:
            self.position = Point(x=latitude, y=longitude, srid=4326)
        self.from_date = None
        self.to_date = None
        self.state = None
