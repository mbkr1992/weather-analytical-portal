ALTER TABLE file_meta
ADD COLUMN is_parsed boolean NOT NULL  default False

﻿-- select * from satellite_data limit 100
-- select * from satellite_data where ST_INTERSECTS(position, ST_GeomFromText('POINT(49 10)', 4326));
select * from satellite_data where ST_INTERSECTS(position, ST_GeomFromText('POLYGON((49 12, 49 6, 48 11, 49 12))', 4326));

