-- noinspection SqlNoDataSourceInspectionForFile

-- noinspection SqlDialectInspectionForFile

INSERT INTO attribute (id, name, name_original, description, unit)
VALUES ( 1, 'Measurement date', 'MESS_DATUM', 'end of interval in UTC', 'yyyymmddhh:mm'),
(2, 'Quality', 'QN_592', 'quality level of next columns', 'coding see paragraph /"Quality information/"'),
(3, 'ATMO Radiation', 'ATMO_LBERG', 'hourly sum of longwave downward radiation', 'J/cm^2'),
(4, 'FD Raiation', 'FD_LBERG', 'hourly sum of diffuse solar radiation', 'J/cm^2'),
(5, 'FG Radiation', 'FG_LBERG', 'hourly sum of solar incoming radiation', 'J/cm^2'),
(6, 'SD Radiation', 'SD_LBERG', 'hourly sum of sunshine duration', 'min'),
(7, 'Zenith', 'ZENIT', 'quality level of next columns', 'degree'),
(8, 'Measurement date solar', 'MESS_DATUM_WOZ', 'end of interval in local true solar time', 'yyyymmddhh:mm');