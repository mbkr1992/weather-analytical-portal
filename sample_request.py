from operation_model.german_weather_operation import GermanWeatherOperation
from operation_model.copernicus_operation import CopernicusOperation


from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()
# server.retrieve(
#     {
#         "class": "mc",
#         "dataset": "cams_nrealtime",
#         "date": "2018-01-01/to/2018-01-31",
#         "expver": "0001",
#         "levtype": "sfc",
#         "param": "167.128",
#         "step": "0",
#         "stream": "oper",
#         "time": "00:00:00",
#         "type": "an",
#         "target": "output1.grib",
#     })

# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2017-01-01/to/2017-01-31",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "4.218/6.218/13.218/16.218/27.218/30.218/34.128/45.218/47.218/52.210/53.210/125.210/126.210/127.210/128.210/129.128/137.128/151.128/164.128/165.128/166.128/167.128/168.128/172.128/174.128/186.128/187.128/188.128/206.210",
#     "step": "0",
#     "stream": "oper",
#     "time": "00:00:00",
#     "type": "an",
#     "area": "europe",
#     "target": "all-output1-europe.grib",
# })
# print('REQUEST 1')

# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2017-01-01/to/2017-01-31",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "2.214/3.214/4.218/6.218/13.218/16.218/27.218/30.218/34.128/45.218/47.218/52.210/53.210/72.210/73.210/74.210/125.210/126.210/127.210/128.210/137.128/142.128/143.128/144.128/151.128/164.128/165.128/166.128/167.128/168.128/172.128/186.128/187.128/188.128/206.210/207.210/208.210/209.210/210.210/211.210/212.210/213.210/214.210/215.210/216.210/243.128",
#     "stream": "oper",
#     "time": "00:00:00",
#     "step": "0/3/6/9/12/15/21",
#     "type": "fc",
#     "area": "europe",
#     "target": "cams_nrealtime.fc.europe.grib",
# })
# print('REQUEST 2')

# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2018-01-01/to/2018-01-31",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "168.128",
#     "step": "0",
#     "stream": "oper",
#     "time": "00:00:00/06:00:00/12:00:00/18:00:00",
#     "type": "an",
#     "target": "output.an.[time].grib",
# })
# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2018-01-01/to/2018-01-31",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "168.128",
#     "step": "0",
#     "stream": "oper",
#     "time": "00:00:00/12:00:00",
#     "type": "fc",
#     "target": "output.fc.[time].grib",
# })
# print('REQUEST 1')

# server.retrieve({
#     'stream'    : "oper",
#     'levtype'   : "sfc",
#     'param'     : "aod550",
#     'dataset'   : "cams_nrealtime",
#     'step'      : "0/3/6/9/12/15/18/21/24/27/30/33/36/39/42/45/48/51/54/57/60/63/66/69/72/75/78/81/84/87/90/93/96/99/102/105/108/111/114/117/120",
#     'expver'    : "0001",
#     'time'      : "00",
#     'date'      : "2016-06-01/to/2016-06-11",
#     'type'      : "fc",
#     'class'     : "mc",
#     'target'    : "cams_nrealtime.grib"
# })

# paths_to_parse = [
#     # '/pub/CDC/observations_germany/climate/1_minute/precipitation/historical/1993/1minutenwerte_nieder_00003_19930701_19930731_hist.zip',
#     # '/pub/CDC/observations_germany/climate/10_minutes/air_temperature/historical/10minutenwerte_tu_00003_19930428_19991231_hist.zip',
#     # '/pub/CDC/observations_germany/climate/10_minutes/extreme_temperature/historical/10minutenwerte_tx_00003_19930428_19991231_hist.zip',
#     # '/pub/CDC/observations_germany/climate/10_minutes/extreme_wind/historical/10minutenwerte_fx_00003_19930428_19991231_hist.zip',
#     # '/pub/CDC/observations_germany/climate/10_minutes/precipitation/historical/10minutenwerte_rr_00003_19930428_19991231_hist.zip',
#     # '/pub/CDC/observations_germany/climate/10_minutes/solar/historical/10minutenwerte_solar_00003_19930428_19991231_hist.zip'
#     # '/pub/CDC/observations_germany/climate/10_minutes/wind/historical/10minutenwerte_ff_00003_19930428_19991231_hist.zip',
#     # '/pub/CDC/observations_germany/climate/daily/kl/historical/tageswerte_KL_00001_19370101_19860630_hist.zip',
#     # '/pub/CDC/observations_germany/climate/daily/more_precip/historical/tageswerte_RR_00001_19120101_19860630_hist.zip',
#     # '/pub/CDC/observations_germany/climate/daily/soil_temperature/historical/tageswerte_EB_00003_19510101_20110331_hist.zip',
#     # '/pub/CDC/observations_germany/climate/daily/solar/tageswerte_ST_00183_row.zip',
#     # '/pub/CDC/observations_germany/climate/daily/water_equiv/historical/tageswerte_Wa_00001_19370130_19580125_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/air_temperature/historical/stundenwerte_TU_00003_19500401_20110331_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/cloudiness/historical/stundenwerte_N_00003_19500401_20110401_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/precipitation/historical/stundenwerte_RR_00003_19500401_20110401_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/pressure/historical/stundenwerte_P0_00003_19500401_20110401_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/soil_temperature/historical/stundenwerte_EB_00003_19510101_20110331_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/solar/stundenwerte_ST_00183_row.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/sun/historical/stundenwerte_SD_00003_19510101_20110331_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/visibility/historical/stundenwerte_VV_00003_19500401_20110401_hist.zip',
#     # '/pub/CDC/observations_germany/climate/hourly/wind/historical/stundenwerte_FF_00003_19370101_20110331_hist.zip',
#     # '/pub/CDC/observations_germany/climate/monthly/kl/historical/monatswerte_KL_00001_19310101_19860630_hist.zip',
#     # '/pub/CDC/observations_germany/climate/monthly/more_precip/historical/monatswerte_RR_00001_18910101_19860630_hist.zip',
# ];


def perform_async_operation():
    # GermanWeatherOperation().perform_operation()
    CopernicusOperation().perform_operation()
    pass


def main():
    perform_async_operation()

main()

