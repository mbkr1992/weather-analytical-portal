class Cloudiness:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_8 = None      # small int
        self.v_n_i = None  # real
        self.v_n = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_8,
                self.v_n_i,
                self.v_n)

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
