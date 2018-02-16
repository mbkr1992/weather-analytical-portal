class Kl:

    def __init__(self):
        super().__init__()

        self.station_id = None
        self.measurement_date = None
        self.measurement_category = None

        self.qn_3 = None    # small int

        self.fx = None  # small int
        self.fm = None      # small int
        self.qn_4 = None  # small int

        self.rsk = None  # real
        self.rskf = None       # small int

        self.sdk = None   # real
        self.shk_tag = None      # small int
        self.nm = None  # real
        self.vpm = None  # real
        self.pm = None    # small int
        self.tmk = None  # real
        self.upm = None  # real
        self.txk = None  # real
        self.tnk = None  # real
        self.tgk = None  # real

        self.mo_n = None
        self.mo_tt = None
        self.mo_tx = None
        self.mo_tn = None
        self.mo_fk = None
        self.mx_tx = None
        self.mx_fx = None
        self.mx_tn = None
        self.mo_sd_s = None
        self.qn_6 = None
        self.mo_rr = None
        self.mx_rs = None


    def __str__(self):
        return '{0}, {1}'.format(self.station_id, self.measurement_date)
