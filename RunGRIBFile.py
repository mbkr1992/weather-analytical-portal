import pygrib

grbs = pygrib.open('output')

grbs.seek(0)

# for grb in grbs:
#     print(grb)

grb = grbs.select(name='2 metre temperature')[0]
temperature = grb.values # same as grb['values']

# print(grb.values)
# print(grb.keys())
# print('Temperature', temperature.shape, temperature.min(), temperature.max())

data, lats, lons = grb.data(lat1=48,lat2=49,lon1=13,lon2=14)
# print(data.shape, lats.min(), lats.max(), lons.min(), lons.max())
for key in grb.keys():
    print('Key: {}, Value: {}'.format(key, grb[key]))
# print('Data', grb.keys())
# print('Data Date', grb.dataDate)
# lats, lons = grb.latlons()
# print(lats.shape, lats.min(), lats.max(), lons.shape, lons.min(), lons.max())
grbs.close()