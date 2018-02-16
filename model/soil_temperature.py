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
        self.v_te100m = None  # real

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
