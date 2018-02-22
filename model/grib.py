import json
import sys


class GribEncoder(json.JSONEncoder):

    def default(self, o):
        return o.__dict__


class Grib(GribEncoder):

    def __init__(self, name, value, timestamp, latitude, longitude, unit, mars_type, mars_class, param_id, max, min):
        super().__init__()

        self.name = name
        self.value = value
        self.timestamp = timestamp

        self.latitude = latitude
        self.longitude = longitude
        self.unit = unit  # str
        self.mars_type = mars_type
        self.mars_class = mars_class
        self.param_id = param_id
        self.max = max
        self.min = min

    def __str__(self):
        return '{0}, {1}, {2}'.format(self.name, self.value, self.timestamp)

    # def __repr__(self):
    #     return json.dumps(dict(
    #         name=self.name,
    #         value=self.value,
    #         timestamp=self.timestamp,
    #         unit=self.unit,
    #         mars_type=self.mars_type,
    #         mars_class=self.mars_class,
    #         param_id=self.param_id,
    #         max=self.max,
    #         min=self.min,
    #     ))

    # def to_json(self):
    #     print(json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4))
    #     sys.exit()
    #     return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
