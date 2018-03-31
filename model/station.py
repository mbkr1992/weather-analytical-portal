from postgis import Point


class Station:

    def __init__(self, longitude, latitude):
        super().__init__()

        self.id = id
        self.name = None
        self.height = None
        self.latitude = latitude
        self.longitude = longitude
        self.position = Point(x=longitude, y=latitude, srid=4326)
        self.from_date = None
        self.to_date = None
        self.state = None
