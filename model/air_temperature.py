class AirTemperature:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_9 = None      # small int
        self.tt_tu = None  # real
        self.rf_tu = None  # real

        self.qn = None
        self.pp = None
        self.tt = None
        self.tm5 = None
        self.rf = None
        self.td = None

        self.tx = None
        self.tn = None
        self.tn5 = None

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
