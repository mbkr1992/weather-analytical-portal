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
        self.pp_10 = None
        self.tt_10 = None
        self.tm5_10 = None
        self.rf_10 = None
        self.td_10 = None

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
