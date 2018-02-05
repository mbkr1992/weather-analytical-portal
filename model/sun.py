class Sun:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_7 = None      # small int
        self.sd_so = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_7,
                self.sd_so)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
