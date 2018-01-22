
class Solar:

    def __init__(self, solar=None):
        super().__init__()

        self.station_id = solar['STATIONS_ID']
        self.measurement_date = solar['MESS_DATUM']
        self.qn = solar['QN_592']
        self.atmo_radiation = solar['ATMO_LBERG']
        self.fd_radiation = solar['FD_LBERG']
        self.fg_radiation = solar['FG_LBERG']
        self.sd_radiation = solar['SD_LBERG']
        self.zenith = solar['ZENIT']
        self.measurement_date_solar = solar['MESS_DATUM_WOZ']

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.station_id, self.measurement_date, self.qn)