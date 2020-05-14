## SQL statements to create database tables
SQL_CREATE_TABLES <- list(
  # Synoptic data records
  #
  create_synoptic_table = glue::glue(
    "CREATE TABLE Synoptic (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    year INTEGER NOT NULL,         -- Four digit year
    month INTEGER NOT NULL,        -- Month: 1-12
    day INTEGER NOT NULL,          -- Day: 1-31
    hour INTEGER NOT NULL,         -- Hour: 0-23
    minute INTEGER NOT NULL,       -- Minute: 0-59
    precipitation REAL,            -- Rainfall (mm)
    temperature REAL,              -- Temperature (degrees Celsius)
    relhumidity REAL,              -- Relative humidity (percentage)
    windspeed REAL,                -- Wind speed (km/h)
    winddir REAL,                  -- Wind direction (degrees)
    tmaxdaily REAL DEFAULT NULL,   -- Maximum temperature per calendar day
    precipdaily REAL DEFAULT NULL, -- Total rainfall between 09:01 and 09:00 next day
    kbdi REAL DEFAULT NULL,        -- Keetch-Bryam drought index based on average rainfall
                                   --   at the station for 2001-2015
    drought REAL DEFAULT NULL,     -- Drought factor
    ffdi REAL DEFAULT NULL,        -- Forest fire danger index

    UNIQUE(station, year, month, day, hour, minute),

    CHECK (month >= 1 AND month <= 12),
    CHECK (day >= 1),
    CHECK (day <= 29 OR
           (day = 30 AND month <> 2) OR
           (day = 31 AND month IN (1,3,5,7,8,10,12))),
    CHECK (hour >= 0 AND hour <= 23),
    CHECK (minute >= 0 AND minute <= 59) );"),


  # AWS data - differs only from Synoptic data in more frequent time steps
  # and the additional windgust variable, but it is convenient to have a
  # separate table in the database.
  #
  create_aws_table = glue::glue(
    "CREATE TABLE AWS (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    year INTEGER NOT NULL,         -- Four digit year
    month INTEGER NOT NULL,        -- Month: 1-12
    day INTEGER NOT NULL,          -- Day: 1-31
    hour INTEGER NOT NULL,         -- Hour: 0-23
    minute INTEGER NOT NULL,       -- Minute: 0-59
    precipitation REAL,            -- Rainfall (mm)
    temperature REAL,              -- Temperature (degrees Celsius)
    relhumidity REAL,              -- Relative humidity (percentage)
    windspeed REAL,                -- Wind speed (km/h)
    winddir REAL,                  -- Wind direction (degrees)
    windgust REAL,                 -- Maximum wind speed in last 10 minutes (km/h)
    tmaxdaily REAL DEFAULT NULL,   -- Maximum temperature per calendar day
    precipdaily REAL DEFAULT NULL, -- Total rainfall between 09:01 and 09:00 next day
    kbdi REAL DEFAULT NULL,        -- Keetch-Bryam drought index based on average rainfall
                                   --   at the station for 2001-2015
    drought REAL DEFAULT NULL,     -- Drought factor
    ffdi REAL DEFAULT NULL,        -- Forest fire danger index

    UNIQUE(station, year, month, day, hour, minute),

    CHECK (month >= 1 AND month <= 12),
    CHECK (day >= 1),
    CHECK (day <= 29 OR
           (day = 30 AND month <> 2) OR
           (day = 31 AND month IN (1,3,5,7,8,10,12))),
    CHECK (hour >= 0 AND hour <= 23),
    CHECK (minute >= 0 AND minute <= 59) );"),

  # BOM weather station metadata
  #
  create_stations_table = glue::glue(
    "CREATE TABLE Stations (
    station INTEGER NOT NULL,      -- Integer station identifier assigned by BOM
    name TEXT NOT NULL,            -- Station name
    start TEXT NOT NULL,           -- Start of operation: month and year
    aws INTEGER NOT NULL,          -- 1: automatic weather station; 0: synoptic only
    state TEXT,                    -- State or territory (plus Antarctica)
    lon REAL NOT NULL,             -- Longitude (decimal degrees)
    lat REAL NOT NULL,             -- Latitude (decimal degrees)
    annualprecip_narclim REAL,     -- NARCLIM p12 average annual precipitation

    UNIQUE(station),

    CHECK (aws IN (0,1)),
    CHECK (lon > 0),
    CHECK (lat < 0) );")
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
