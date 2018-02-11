from operation_model.german_weather_operation import GermanWeatherOperation

from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()
server.retrieve(
    {
        "class": "mc",
        "dataset": "cams_nrealtime",
        "date": "2018-01-01/to/2018-01-31",
        "expver": "0001",
        "levtype": "sfc",
        "param": "167.128",
        "step": "0",
        "stream": "oper",
        "time": "00:00:00",
        "type": "an",
        "target": "output1.grib",
    })

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
    GermanWeatherOperation().perform_operation()
    pass

def main():
    pass

main()