class AirTemperature:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None

        self.qn_9 = None      # small int
        self.tt_tu = None  # real
        self.rf_tu = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_9,
                self.tt_tu,
                self.rf_tu)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
