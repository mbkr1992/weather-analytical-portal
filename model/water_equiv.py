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

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
