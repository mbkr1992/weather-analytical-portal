
class Solar:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.qn = None
        self.atmo_radiation = None
        self.fd_radiation = None
        self.fg_radiation = None
        self.sd_radiation = None
        self.zenith = None
        self.measurement_date_local = None
        self.measurement_category = None

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn,
                self.atmo_radiation,
                self.fd_radiation,
                self.fg_radiation,
                self.sd_radiation,
                self.zenith,
                self.measurement_date_local)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
