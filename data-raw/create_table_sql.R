## SQL statements to create database tables
SQL_CREATE_TABLES <- list(
  # Synoptic data records
  #
  create_synoptic_table = glue::glue(
    "CREATE TABLE Synoptic (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    date_local TEXT NOT NULL,      -- 'yyyy-mm-dd' local (daylight saving) date
    hour_local INTEGER NOT NULL,   -- Hour local time: 0-23
    min_local INTEGER NOT NULL,    -- Minute local time: 0-59
    date_std TEXT NOT NULL,        -- 'yyyy-mm-dd' standard date
    hour_std INTEGER NOT NULL,     -- Hour standard time: 0-23
    min_std INTEGER NOT NULL,      -- Minute standard time: 0-59
    precipitation REAL,            -- Rainfall (mm)
    precipitation_quality CHAR(1), -- Rainfall quality code
    temperature REAL,              -- Temperature (degrees Celsius)
    temperature_quality CHAR(1),   -- Temperature quality code
    relhumidity REAL,              -- Relative humidity (percentage)
    relhumidity_quality CHAR(1),   -- Relative humidity quality code
    windspeed REAL,                -- Wind speed (km/h)
    windspeed_quality CHAR(1),     -- Wind speed quality code
    winddir REAL,                  -- Wind direction (degrees)
    winddir_quality CHAR(1),       -- Wind direction quality code
    tmaxdaily REAL DEFAULT NULL,   -- Maximum temperature per calendar day
    precipdaily REAL DEFAULT NULL, -- Total rainfall between 09:01 and 09:00 next day
    kbdi REAL DEFAULT NULL,        -- Keetch-Bryam drought index based on average rainfall
                                   --   at the station for 2001-2015
    drought REAL DEFAULT NULL,     -- Drought factor
    ffdi REAL DEFAULT NULL,        -- FFDI: Forest fire danger index
    ffdi_quality CHAR(1),          -- FFDI quality code

    UNIQUE(station, date_local, hour, minute) );"),

  # AWS data - differs only from Synoptic data in more frequent time steps
  # and the additional windgust variable, but it is convenient to have a
  # separate table in the database.
  #
  create_aws_table = glue::glue(
    "CREATE TABLE AWS (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    date_local TEXT NOT NULL,      -- 'yyyy-mm-dd' local (daylight saving) date
    hour_local INTEGER NOT NULL,   -- Hour local time: 0-23
    min_local INTEGER NOT NULL,    -- Minute local time: 0-59
    date_std TEXT NOT NULL,        -- 'yyyy-mm-dd' standard date
    hour_std INTEGER NOT NULL,     -- Hour standard time: 0-23
    min_std INTEGER NOT NULL,      -- Minute standard time: 0-59
    precipitation REAL,            -- Rainfall (mm)
    precipitation_quality CHAR(1), -- Rainfall quality code
    temperature REAL,              -- Temperature (degrees Celsius)
    temperature_quality CHAR(1),   -- Temperature quality code
    relhumidity REAL,              -- Relative humidity (percentage)
    relhumidity_quality CHAR(1),   -- Relative humidity quality code
    windspeed REAL,                -- Wind speed (km/h)
    windspeed_quality CHAR(1),     -- Wind speed quality code
    winddir REAL,                  -- Wind direction (degrees)
    winddir_quality CHAR(1),       -- Wind direction quality code
    windgust REAL,                 -- Maximum wind speed in last 10 minutes (km/h)
    windgust_quality CHAR(1),      -- Wind gust quality code
    tmaxdaily REAL DEFAULT NULL,   -- Maximum temperature per calendar day
    precipdaily REAL DEFAULT NULL, -- Total rainfall between 09:01 and 09:00 next day
    kbdi REAL DEFAULT NULL,        -- Keetch-Bryam drought index based on average rainfall
                                   --   at the station for 2001-2015
    drought REAL DEFAULT NULL,     -- Drought factor
    ffdi REAL DEFAULT NULL,        -- FFDI: Forest fire danger index
    ffdi_quality CHAR(1),          -- FFDI quality code

    UNIQUE(station, date_local, hour, minute) );"),

  # BOM weather station metadata
  #
  create_stations_table = glue::glue(
    "CREATE TABLE Stations (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    name TEXT NOT NULL,            -- Station name
    state TEXT,                    -- State or territory (plus Antarctica)
    startyear INTEGER NOT NULL,    -- First year of station operation
    startmonth integer,            -- First month of station operation (1-12)
    aws INTEGER NOT NULL,          -- 1: automatic weather station; 0: synoptic only
    annualprecip_narclim REAL,     -- NARCLIM p12 average annual precipitation
    lon REAL NOT NULL,             -- Longitude (decimal degrees)
    lat REAL NOT NULL,             -- Latitude (decimal degrees)

    UNIQUE(station),

    CHECK (aws IN (0,1)),
    CHECK (lon > 0),
    CHECK (lat < 0),
    CHECK (state IN ('ACT', 'ANT', 'NSW', 'NT', 'OS',
                     'QLD', 'SA', 'TAS', 'VIC', 'WA')));")
)


## Flags for database connection states used by private function .ensure_connection
.CON_FLAGS <- list(
  Open = 1,
  HasTables = 2,
  HasData = 4
)


usethis::use_data(SQL_CREATE_TABLES, overwrite = TRUE)
usethis::use_data(.CON_FLAGS, internal = TRUE, overwrite = TRUE)

rm(SQL_CREATE_TABLES, .CON_FLAGS)
