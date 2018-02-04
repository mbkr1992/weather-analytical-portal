class MorePrecip:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_6 = None    # small int

        self.rs = None  # real
        self.rsf = None      # real
        self.sh_tag = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_6,
                self.rs,
                self.rsf,
                self.sh_tag)

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
