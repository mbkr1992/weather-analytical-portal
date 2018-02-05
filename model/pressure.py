class Pressure:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_8 = None      # small int
        self.p = None  # real
        self.p0 = None

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_8,
                self.p,
                self.p0)

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
