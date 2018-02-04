class WaterEquiv:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None

        self.qn_6 = None      # small int
        self.ash_6 = None  # real
        self.sh_tag = None  # real
        self.wash_6 = None  # real
        self.waas_6 = None  # real

    def to_tuple(self):
        return (self.station_id,
                self.measurement_date,
                self.measurement_category,
                self.qn_6,
                self.ash_6,
                self.sh_tag,
                self.wash_6,
                self.waas_6)

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date)
