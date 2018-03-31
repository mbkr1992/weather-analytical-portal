class WaterEquiv:

    def __init__(self,
                 station_id=None,
                 date=None,
                 interval=None,
                 name=None,
                 value=None,
                 unit=None,
                 information=None):

        super().__init__()

        self.station_id = station_id
        self.date = date
        self.interval = interval
        self.name = name
        self.value = value
        self.unit = unit
        self.information = information
        self.source = 'DWD'

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.name)
