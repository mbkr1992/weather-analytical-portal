class Wind:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None

        self.qn_3 = None      # small int
        self.f = None  # real
        self.d = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_3,
                self.f,
                self.d,)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
