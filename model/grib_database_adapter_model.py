class GDBAdapter:

    def __init__(self, date=None, latitude=None, longitude=None, information=None, meta_information=None):
        super().__init__()

        self.date = date
        self.latitude = latitude
        self.longitude = longitude
        self.information = information
        self.meta_information = meta_information

    def __str__(self):
        return '{0}, {1}'.format(self.date, len(self.information))
