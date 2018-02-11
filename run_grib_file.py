import pygrib
import numpy as np

grbs = pygrib.open('output.grib')

grbs.seek(0)

for grb in grbs:
    data, lats, lons = grb.data(lat1=48, lat2=50, lon1=10, lon2=14)
    for (x, y), value in np.ndenumerate(data):
        print('Lat {0}, lng {1}, value {2}, recorded at: {3}'.format(lats[x, y], lons[x, y], value, grb.dataDate))

# grb = grbs.select(name='2 metre temperature')[0]


# print('Here {0}'.format(grb))

# temperature = grb.values
# print('Temperature', temperature.shape, temperature.min(), temperature.max())

# lats, long = grb.latlons()
# print('Lat', lats.shape, lats.min(), lats.max())
# print('keys', grb.keys())

# for key in grb.keys():
#     print('Key: {}, Value: {}'.format(key, grb[key]))
# print('Data', grb.keys())
# print('Data timestamp', grb.dataDate, grb.dataTime)
# lats, lons = grb.latlons()
# print(lats.shape, lats.min(), lats.max(), lons.shape, lons.min(), lons.max())
grbs.close()