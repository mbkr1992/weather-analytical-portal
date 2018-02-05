class Wind:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_3 = None      # small int
        self.f = None  # real
        self.d = None  # real

        self.qn = None
        self.ff_10 = None
        self.dd_10 = None

        self.fx_10 = None
        self.fnx_10 = None
        self.fmx_10 = None
        self.dx_10 = None

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_3,
                self.f,
                self.d,
                self.qn,
                self.ff_10,
                self.dd_10,
                self.fx_10,
                self.fnx_10,
                self.fmx_10,
                self.dx_10)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
