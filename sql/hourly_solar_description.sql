INSERT INTO attribute (name, name_original, description, unit)
VALUES ('Measurement date', 'MESS_DATUM', 'end of interval in UTC', 'yyyymmddhh:mm'),
('Quality', 'QN_592', 'quality level of next columns', 'coding see paragraph /"Quality information/"'),
('ATMO Radiation', 'ATMO_LBERG', 'hourly sum of longwave downward radiation', 'J/cm^2'),
('FD Raiation', 'FD_LBERG', 'hourly sum of diffuse solar radiation', 'J/cm^2'),
('FG Radiation', 'FG_LBERG', 'hourly sum of solar incoming radiation', 'J/cm^2'),
('SD Radiation', 'SD_LBERG', 'hourly sum of sunshine duration', 'min'),
('Zenith', 'ZENIT', 'quality level of next columns', 'degree'),
('Measurement date solar', 'MESS_DATUM_WOZ', 'end of interval in local true solar time', 'yyyymmddhh:mm');