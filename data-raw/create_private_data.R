## SQL statements to create database tables

.BOM_SQL <- list(
  create_synoptic_table = paste(
    "CREATE TABLE Synoptic (",
    "`station` INTEGER,",
    "`year` INTEGER,",
    "`month` INTEGER,",
    "`day` INTEGER,",
    "`hour` INTEGER,",
    "`minute` INTEGER,",
    "`precipitation` REAL,",
    "`temperature` REAL,",
    "`relhumidity` REAL,",
    "`windspeed` REAL,",
    "`winddir` REAL,",
    "PRIMARY KEY(station, year, month, day, hour) );",
    sep = "\n"),

  create_aws_table = paste(
    "CREATE TABLE AWS (",
    "`station` INTEGER,",
    "`year` INTEGER,",
    "`month` INTEGER,",
    "`day` INTEGER,",
    "`hour` INTEGER,",
    "`minute` INTEGER,",
    "`precipitation` REAL,",
    "`temperature` REAL,",
    "`relhumidity` REAL,",
    "`windspeed` REAL,",
    "`winddir` REAL,",
    "`windgust` REAL,",
    "PRIMARY KEY(station, year, month, day, hour, minute) );",
    sep = "\n")
)


## Flags for database connection states used by private function .ensure_connection

.CON_FLAGS <- list(
  Open = 1,
  HasTables = 2,
  HasData = 4
)

devtools::use_data(.BOM_SQL, .CON_FLAGS, internal = TRUE, overwrite = TRUE)
rm(.BOM_SQL, .CON_FLAGS)
