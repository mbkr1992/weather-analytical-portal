from mapper_model import mapper


class Station:

    def __init__(self, station=None):
        super().__init__()

        self.id = station['Stations_id']
        self.name = station['Stationsname']
        self.height = station['Stationshoehe']
        self.latitude = station['geoBreite']
        self.longitude = station['geoLaenge']
        self.from_date = station['von_datum']
        self.to_date = station['bis_datum']
        self.state = station['Bundesland']