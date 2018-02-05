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

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_9,
                self.tt_tu,
                self.rf_tu,
                self.qn,
                self.pp_10,
                self.tt_10,
                self.tm5_10,
                self.rf_10,
                self.td_10)

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
