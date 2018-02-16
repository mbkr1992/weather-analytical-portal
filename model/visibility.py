class Visibility:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_8 = None      # small int
        self.v_vv_i = None  # real
        self.v_vv = None  # real

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
