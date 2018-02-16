
class Solar:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.qn = None
        self.atmo_radiation = None
        self.fd_radiation = None
        self.fg_radiation = None
        self.sd_radiation = None
        self.zenith = None
        self.measurement_date_local = None
        self.measurement_category = None
        self.ds_10 = None
        self.gs_10 = None
        self.sd_10 = None
        self.ls_10 = None

    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
