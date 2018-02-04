class SoilTemperature:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None

        self.qn_2 = None      # small int
        self.v_te002m = None  # real
        self.v_te005m = None  # real
        self.v_te010m = None  # real
        self.v_te020m = None  # real
        self.v_te050m = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_2,
                self.v_te002m,
                self.v_te005m,
                self.v_te010m,
                self.v_te020m,
                self.v_te050m)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
