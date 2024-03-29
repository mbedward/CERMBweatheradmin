-------------------------------------------
-- Create new database for BOM station data
-------------------------------------------

CREATE DATABASE cermb_weather
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_Australia.1252'
    LC_CTYPE = 'English_Australia.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
	
-- Add PostGIS support for spatial data and queries
CREATE EXTENSION postgis;


-------------------------------------------------------
-- Create the 'bom' schema to hold all tables and views
-------------------------------------------------------

CREATE SCHEMA bom
    AUTHORIZATION postgres;

GRANT ALL ON SCHEMA bom TO postgres;

GRANT USAGE ON SCHEMA bom TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom
GRANT SELECT ON TABLES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom
GRANT USAGE ON SEQUENCES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom
GRANT EXECUTE ON FUNCTIONS TO readonly_user;


-- Set the search path for users to have the bom
-- schema in first position

ALTER DATABASE cermb_weather SET search_path TO bom, "$user", public;


-------------------------------------------------------------------
-- Creates a 'readonly' role to be inherited by all standard users.
-- This assumes that the schema 'bom' exists.
-------------------------------------------------------------------
CREATE ROLE readonly_user;

GRANT CONNECT ON DATABASE cermb_weather TO readonly_user;

GRANT USAGE ON SCHEMA bom TO readonly_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bom TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA bom TO readonly_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bom TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom GRANT SELECT ON TABLES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom GRANT USAGE ON SEQUENCES TO readonly_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bom GRANT EXECUTE ON FUNCTIONS TO readonly_user;


-- Example of how to create a standard user
-- CREATE ROLE somebody LOGIN INHERIT PASSWORD 'some_password' IN ROLE readonly_user;


--------------------------------
-- Create the BOM stations table
--------------------------------
CREATE TABLE bom.stations
(
    station integer NOT NULL,
    name text NOT NULL,
    state text NOT NULL,
    startyear integer,
    startmonth integer,
    aws boolean NOT NULL,
    annualprecip_narclim real,
    geom geometry(Point,4283) NOT NULL,
    CONSTRAINT stations_pkey PRIMARY KEY (station),
    CONSTRAINT stations_state_check CHECK (state = ANY (ARRAY['ACT', 'ANT', 'NSW', 'NT', 'OS', 'QLD', 'SA', 'TAS', 'VIC', 'WA']))
)
TABLESPACE pg_default;

ALTER TABLE bom.stations
    OWNER to postgres;

GRANT ALL ON TABLE bom.stations TO postgres;

-- Spatial index on station location
CREATE INDEX idx_stations_geom
    ON bom.stations USING gist
    (geom)
    TABLESPACE pg_default;

-- Index on station number
CREATE INDEX idx_stations_station
    ON bom.stations USING btree
    (station ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index on state
CREATE INDEX idx_stations_state
    ON bom.stations USING btree
    (state ASC NULLS LAST)
    TABLESPACE pg_default;

	
---------------------------------
-- Create the synoptic data table
---------------------------------

CREATE TABLE bom.synoptic
(
    id serial PRIMARY KEY,
    station integer NOT NULL,
    date_local date NOT NULL,
    hour_local integer NOT NULL,
    min_local integer NOT NULL,
    date_std date NOT NULL,
    hour_std integer NOT NULL,
    min_std integer NOT NULL,
    precipitation real,
    precipitation_quality character(1),
    temperature real,
    temperature_quality character(1),
    relhumidity real,
    relhumidity_quality character(1),
    windspeed real,
    windspeed_quality character(1),
    winddir real,
    winddir_quality character(1),
    tmaxdaily real,
    precipdaily real,
    kbdi real,
    drought real,
    ffdi real,
    ffdi_quality character(1),
	
    CONSTRAINT synoptic_station_datetime_key UNIQUE (station, date_local, hour_local, min_local)
)
TABLESPACE pg_default;

ALTER TABLE bom.synoptic
    OWNER to postgres;
	
GRANT ALL ON TABLE bom.synoptic TO postgres;

-- Index on station number

CREATE INDEX idx_synoptic_station
    ON bom.synoptic USING btree
    (station ASC)
    TABLESPACE pg_default;

-- Index on local date (accounting for daylight saving time)

CREATE INDEX idx_synoptic_date_local
    ON bom.synoptic USING btree
    (date_local ASC)
    TABLESPACE pg_default;

-- Index on standard date (ignoring daylight saving time)

CREATE INDEX idx_synoptic_date_std
    ON bom.synoptic USING btree
    (date_std ASC)
    TABLESPACE pg_default;
	
-- Index on non-null FFDI value

CREATE INDEX idx_synoptic_has_ffdi
	ON bom.synoptic USING btree
	(ffdi) WHERE ffdi IS NOT NULL;


----------------------------
-- Create the AWS data table
----------------------------

CREATE TABLE bom.aws
(
    id serial primary key,
    station integer NOT NULL,
    date_local date NOT NULL,
    hour_local integer NOT NULL,
    min_local integer NOT NULL,
    date_std date NOT NULL,
    hour_std integer NOT NULL,
    min_std integer NOT NULL,
    precipitation real,
    precipitation_quality character(1),
    temperature real,
    temperature_quality character(1),
    relhumidity real,
    relhumidity_quality character(1),
    windspeed real,
    windspeed_quality character(1),
    windgust real,
    windgust_quality character(1),
    winddir real,
    winddir_quality character(1),
    tmaxdaily real,
    precipdaily real,
    kbdi real,
    drought real,
    ffdi real,
    ffdi_quality character(1),
    
    CONSTRAINT aws_station_datetime_key UNIQUE (station, date_local, hour_local, min_local)
)
TABLESPACE pg_default;

ALTER TABLE bom.aws
    OWNER to postgres;

GRANT ALL ON TABLE bom.aws TO postgres;

-- Index on station number

CREATE INDEX idx_aws_station
    ON bom.aws USING btree
    (station ASC)
    TABLESPACE pg_default;

-- Index on local date (accounting for daylight saving time)

CREATE INDEX idx_aws_date_local
    ON bom.aws USING btree
    (date_local ASC)
    TABLESPACE pg_default;

-- Index on standard date (ignoring daylight saving time)

CREATE INDEX idx_aws_date_std
    ON bom.aws USING btree
    (date_std ASC)
    TABLESPACE pg_default;

-- Index on non-null FFDI value

CREATE INDEX idx_aws_has_ffdi
	ON bom.aws USING btree
	(ffdi) WHERE ffdi IS NOT NULL;


---------------------------------
-- Create the upperair data table
---------------------------------
CREATE TABLE IF NOT EXISTS bom.upperair
(
    id serial PRIMARY KEY,
    station integer NOT NULL,
    date_local date NOT NULL,
    hour_local integer NOT NULL,
    min_local integer NOT NULL,
    date_std date NOT NULL,
    hour_std integer NOT NULL,
    min_std integer NOT NULL,
    temperature real,
    temperature_quality character(1),
    dewpoint_temperature real,
    dewpoint_temperature_quality character(1),
    relhumidity real,
    relhumidity_quality character(1),
    windspeed real,
    windspeed_quality character(1),
    winddir real,
    winddir_quality character(1),
    pressure real,
    pressure_quality character(1),
    geopotential_height real,
    geopotential_height_quality character(1),
    level_type integer DEFAULT 0,

	CONSTRAINT upperair_station_datetime_key UNIQUE (station, date_local, hour_local, min_local, pressure)
)
TABLESPACE pg_default;

ALTER TABLE bom.upperair
    OWNER to postgres;

GRANT ALL ON TABLE bom.upperair TO postgres;

-- Index on station number

CREATE INDEX idx_upperair_station
    ON bom.upperair USING btree
    (station ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index on local date (accounting for daylight saving time)

CREATE INDEX idx_upperair_date_local
    ON bom.upperair USING btree
    (date_local ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index on standard date (ignoring daylight saving time)

CREATE INDEX idx_upperair_date_std
    ON bom.upperair USING btree
    (date_std ASC NULLS LAST)
    TABLESPACE pg_default;


-------------------------------------------------------
-- Create the synoptic_stations view to provide a quick
-- way of listing all stations with data records.
-------------------------------------------------------

CREATE OR REPLACE VIEW bom.synoptic_stations AS
SELECT s.station,
    s.name,
    s.state,
    first_date,
    last_date
FROM stations s,
LATERAL ( 
    SELECT s1.date_local AS first_date
    FROM synoptic s1
    WHERE s1.station = s.station
    ORDER BY s1.date_local
    LIMIT 1) lat1,
LATERAL ( 
    SELECT s2.date_local AS last_date
    FROM synoptic s2
    WHERE s2.station = s.station
    ORDER BY s2.date_local DESC
    LIMIT 1) lat2
ORDER BY s.state, s.station;
  
ALTER TABLE bom.synoptic_stations
    OWNER TO postgres;

GRANT ALL ON TABLE bom.synoptic_stations TO postgres;


-------------------------------------------------------
-- Create the aws_stations view to provide a quick
-- way of listing all stations with data records.
-------------------------------------------------------

CREATE OR REPLACE VIEW bom.aws_stations AS
SELECT s.station,
    s.name,
    s.state,
    first_date,
    last_date
FROM stations s,
LATERAL ( 
    SELECT a1.date_local AS first_date
    FROM aws a1
    WHERE a1.station = s.station
    ORDER BY a1.date_local
    LIMIT 1) lat1,
LATERAL ( 
    SELECT a2.date_local AS last_date
    FROM aws a2
    WHERE a2.station = s.station
    ORDER BY a2.date_local DESC
    LIMIT 1) lat2
ORDER BY s.state, s.station;

ALTER TABLE bom.aws_stations
    OWNER TO postgres;

GRANT ALL ON TABLE bom.aws_stations TO postgres;


-------------------------------------------------------
-- Create the upperair_stations view to provide a quick
-- way of listing all stations with data records.
-------------------------------------------------------

CREATE OR REPLACE VIEW bom.upperair_stations AS
SELECT s.station,
    s.name,
    s.state,
    first_date,
    last_date
FROM stations s,
LATERAL ( 
    SELECT u1.date_local AS first_date
    FROM upperair u1
    WHERE u1.station = s.station
    ORDER BY u1.date_local
    LIMIT 1) lat1,
LATERAL ( 
    SELECT u2.date_local AS last_date
    FROM upperair u2
    WHERE u2.station = s.station
    ORDER BY u2.date_local DESC
    LIMIT 1) lat2
ORDER BY s.state, s.station;

ALTER TABLE bom.upperair_stations
    OWNER TO postgres;

GRANT ALL ON TABLE bom.upperair_stations TO postgres;


--------------------------------------------------------------
-- Create a materialized view that shows, for each station
-- in the synoptic data table, the first and last dates 
-- (based on local time) of records with non-null FFDI values. 
-- This is used by the external (R) routines that calculate 
-- FFDI for newly added records.
--------------------------------------------------------------

CREATE MATERIALIZED VIEW bom.synoptic_ffdi_dates AS
SELECT syns.station, 
    syns.name, syns.state,
    lat1.first_ffdi_date, lat2.last_ffdi_date
FROM synoptic_stations syns,
LATERAL ( 
    SELECT s1.date_local AS first_ffdi_date
    FROM synoptic s1
    WHERE s1.station = syns.station AND s1.ffdi IS NOT NULL
    ORDER BY s1.date_local
    LIMIT 1) lat1,
LATERAL ( 
    SELECT s2.date_local AS last_ffdi_date
    FROM synoptic s2
    WHERE s2.station = syns.station AND s2.ffdi IS NOT NULL
    ORDER BY s2.date_local DESC
    LIMIT 1) lat2
ORDER BY syns.station
WITH DATA;

ALTER TABLE bom.synoptic_ffdi_dates
    OWNER TO postgres;

GRANT ALL ON TABLE bom.synoptic_ffdi_dates TO postgres;

-- Index the view on station number

CREATE UNIQUE INDEX idx_synoptic_ffdi_station
    ON bom.synoptic_ffdi_dates USING btree
    (station)
    TABLESPACE pg_default;
	

--------------------------------------------------------------
-- Create a materialized view that shows, for each station
-- in the aws data table, the first and last dates 
-- (based on local time) of records with non-null FFDI values. 
-- This is used by the external (R) routines that calculate 
-- FFDI for newly added records.
--------------------------------------------------------------

CREATE MATERIALIZED VIEW bom.aws_ffdi_dates AS
SELECT a.station, 
    a.name, a.state,
    lat1.first_ffdi_date, lat2.last_ffdi_date
FROM aws_stations a,
LATERAL ( 
    SELECT a1.date_local AS first_ffdi_date
    FROM aws a1
    WHERE a1.station = a.station AND a1.ffdi IS NOT NULL
    ORDER BY a1.date_local
    LIMIT 1) lat1,
LATERAL ( 
    SELECT a2.date_local AS last_ffdi_date
    FROM aws a2
    WHERE a2.station = a.station AND a2.ffdi IS NOT NULL
    ORDER BY a2.date_local DESC
    LIMIT 1) lat2
ORDER BY a.station
WITH DATA;

ALTER TABLE bom.aws_ffdi_dates
    OWNER TO postgres;

GRANT ALL ON TABLE bom.aws_ffdi_dates TO postgres;

-- Index the view on station number

CREATE UNIQUE INDEX idx_aws_ffdi_station
    ON bom.aws_ffdi_dates USING btree
    (station)
    TABLESPACE pg_default;


----------------------------------------------
-- Function to assign an FFDI value to one of
-- the standard categories
----------------------------------------------

CREATE OR REPLACE FUNCTION bom.get_ffdi_category(ffdi real)
	RETURNS text
	LANGUAGE sql
AS
$BODY$
	SELECT CASE
		WHEN ffdi < 12 THEN 'low to moderate'
		WHEN ffdi < 25 THEN 'high'
		WHEN ffdi < 50 THEN 'very high'
		WHEN ffdi < 75 THEN 'severe'
		WHEN ffdi < 100 THEN 'extreme'
		WHEN ffdi >= 100 THEN 'catastrophic'
	END;
$BODY$;

ALTER FUNCTION bom.get_ffdi_category(real)
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION bom.get_ffdi_category(real) TO postgres;


-------------------------------------------------------------
-- Functions to calculate vapour pressure deficit.
--
-- Function 'vpd3' returns a tuple of three values:
--   vpactual, vpsaturation, vpdeficit
-- where the units for all values are kPa.
--
-- Example usage:
-- select station, date_local, hour_local, min_local,
--		temperature, relhumidity, (vpd3(temperature, relhumidity)).*
-- from synoptic
-- where station = 68228 and date_local >= '2022-01-01'::date;
--
-- Function 'vpd' is a convenience wrapper to only return
-- the vapour pressure deficit value.
--
-- Example usage:
-- select station, date_local, hour_local, min_local,
--		temperature, relhumidity, vpd(temperature, relhumidity)
-- from synoptic
-- where station = 68228 and date_local >= '2022-01-01'::date;
--
-------------------------------------------------------------

-- Function to return tuple of vpd component values
CREATE OR REPLACE FUNCTION bom.vpd3(IN temperature real, IN relhumidity real,
								   OUT vpactual real, OUT vpsaturation real, OUT vpdeficit real) AS
$$
BEGIN
	vpsaturation := 0.6108 * exp(17.27 * temperature / (temperature + 237.3));
	vpactual := vpsaturation * relhumidity / 100;
	vpdeficit := vpsaturation - vpactual;
END
$$ LANGUAGE 'plpgsql';


-- Function to return just the vp deficit value
CREATE OR REPLACE FUNCTION bom.vpd(temperature real, relhumidity real) RETURNS real AS
$$
	SELECT (bom.vpd3(temperature, relhumidity)).vpdeficit;
$$ LANGUAGE 'sql';

