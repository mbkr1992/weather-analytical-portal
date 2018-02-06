class Precipitation:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None

        # self.measurement_date_start = None
        # self.measurement_date_end = None

        self.qn = None      # small int
        self.qn_8 = None    # small int

        self.rws_dau_10 = None  # small int
        self.rws_10 = None      # real
        self.rws_ind_10 = None  # small int

        self.rs_01 = None       # small int
        self.rs_ind_01 = None   # small int
        self.rs_ind = None      # small int

        self.rth_01 = None  # small int
        self.rwh_01 = None  # real
        self.wrtr = None    # small int
        self.r1 = None

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
