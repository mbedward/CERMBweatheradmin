BOM_SQL <- list(
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
    "`relhumidity` INTEGER,",
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
    "`relhumidity` INTEGER,",
    "`windspeed` INTEGER,",
    "`winddir` INTEGER,",
    "`windgust` INTEGER,",
    "PRIMARY KEY(station, year, month, day, hour, minute) );",
    sep = "\n")
)

devtools::use_data(BOM_SQL, internal = TRUE, overwrite = TRUE)
rm(BOM_SQL)
