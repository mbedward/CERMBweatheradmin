###### Private (non exported) functions and variables

# Checks that an object is a valid database connection (pool object) and
# that the expected weather data tables are present
.ensure_connection <- function(db) {
  if (!.is_connection(db)) stop("Object is not a database connection pool")

  if (!pool::dbIsValid(db))
    stop("Database connection pool has been closed or was not initialized properly")

  # If the R session has been restarted, dbIsValid will return TRUE even
  # though the pool is effectively closed. As a work-around we do a quick
  # sniff query...
  tryCatch(pool::dbListTables(db),
           error = function(e) {
             stop("Database connection pool is not valid, e.g. due to an R restart")
           })

  tbls <- tolower(pool::dbListTables(db))
  Expected <- c("aws", "synoptic")
  found <- Expected %in% tbls

  if (any(!found)) {
    # First close connection to avoid a leaked pool object
    pool::poolClose(db)

    msg1 <- ifelse(sum(!found) == 1, "table", "tables")
    msg2 <- paste(Expected[!found], collapse=", ")
    msg <- glue::glue("Database is missing {msg1} {msg2}")
    stop(msg)
  }
}


# Checks if the 'Stations' table is present and, if not, creates
# and populates it. We assume that the db connection is valid.
.ensure_stations_table <- function(db) {
  tbls <- tolower(pool::dbListTables(db))
  found <- "stations" %in% tbls
  if (!found) {
    message("Adding Stations table to database")

    pool::poolWithTransaction(db, function(conn) {
      DBI::dbExecute(conn, SQL_CREATE_TABLES$create_stations_table)
      DBI::dbAppendTable(conn, "Stations", CERMBweather::STATION_METADATA)
    })
  }
}


.is_connection <- function(db) {
  cl <- inherits(db, c("Pool", "R6"), which = TRUE)
  all(cl == c(1,2))
}


.is_open_connection <- function(db) {
  .is_connection(db) && pool::dbIsValid(db)
}


# Determine if the database connection is SQLite or PostgreSQL
# and issue an error if it is anything else.
.get_db_type <- function(db) {
  con.type <- pool::dbGetInfo(db)$pooledObjectClass
  con.type <- tolower(con.type)
  con.type <- stringr::str_trim(con.type)

  if (con.type == "sqliteconnection") {
    "sqlite"
  } else if (con.type == "pqconnection") {
    "postgresql"
  } else if (con.type == "postgresqlconnection") {
    stop("Please use the 'RPostgres' package instead of 'RPostgreSQL'")
  } else {
    stop("Unsupported connection type: ", con.type)
  }
}


# Selects and renames columns in a data frame of raw data.
# Adds data type (aws or synoptic) and column types as attributes
# of the returned data frame.
#
# Columns: year, month, day are combined into a 'date' column.
# Hour and minute columns are kept separate and no time zone information
# is assigned.
#
.map_fields <- function(dat) {
  type <- .get_bomdata_type(dat)

  lookup <- dplyr::filter(COLUMN_LOOKUP, datatype == type)
  cnames <- tolower(colnames(dat))
  lookupnames <- tolower(lookup[["input"]])
  ii <- match(cnames, lookupnames)
  if (anyNA(ii)) stop("Unrecognized column name(s): ", colnames(dat)[is.na(ii)])

  importnames <- lookup[["importcolname"]][ii]
  ii.keep <- !is.na(importnames)

  dat <- dat[, ii.keep]
  colnames(dat) <- importnames[ii.keep]


  # Combine the separate year, month, day columns for local and standard
  # time into two date columns.
  dat <- dat %>%
    dplyr::mutate(date_local = sprintf("%4d-%02d-%02d",
                                       year_local, month_local, day_local),
                  date_std = sprintf("%4d-%02d-%02d",
                                     year_std, month_std, day_std))

  dat <- dat[, na.omit(lookup$dbcolname)]

  attr(dat, "datatype") <- type

  coltypes <- sapply(colnames(dat), function(cn) {
    i <- match(cn, lookup[["dbcolname"]])
    if (is.na(i)) stop("Data column name missing from lookup table: ", cn)
    lookup[["dbcoltype"]][i]
  })

  attr(dat, "coltypes") <- coltypes

  # Sometimes there are non-numeric values in the measure fields
  # (e.g. whitespace or '###'). Guard against this by converting
  # each measure field to numeric. Non-numeric values will be
  # coerced to NA.
  for (i in 4:length(coltypes)) {
    suppressWarnings(
      if (coltypes[i] == "numeric") {
        if (!is.numeric(dat[[i]])) {
          dat[[i]] <- as.numeric(dat[[i]])
        }
      }
    )
  }

  dat
}


# Determines whether a BOM data set is for AWS or Synoptic data by checking
# for either of two columns only in AWS data (windgust and AWS flag).
# Note: this is using the column names expected in BOM data, not the standard
# names used in the database.
.get_bomdata_type <- function(dat.raw) {
  cnames <- tolower(colnames(dat.raw))
  if (any(stringr::str_detect(cnames, "maximum.windgust|aws.flag"))) {
    "aws"
  } else {
    "synoptic"
  }
}


.is_zip_file <- function(paths) {
  path <- stringr::str_trim(paths)
  stringr::str_detect(tolower(paths), "\\.zip$")
}


.is_directory <- function(paths) {
  file.info(paths)$isdir
}


.get_file_name <- function(paths) {
  x <- stringr::str_split(paths, "[\\\\/]+")
  sapply(x, tail, 1)
}


# guards against double or mixed slashes in constructed paths
# (there must be a more elegant alternative)
.safe_file_path <- function(...) {
  x <- file.path(...)
  x <- stringr::str_split(x, "[\\\\/]+")
  sapply(x, stringr::str_c, collapse = "/")
}


.extract_station_numbers <- function(filenames) {
  x <- stringr::str_extract(filenames, "_Data_\\d+")
  stringr::str_extract(x, "\\d+")
}


# Convert integer year, month, day values to Date objects.
# If year is a data frame or matrix it is assumed to have columns
# year, month and day. Otherwise three equal-length vectors are
# expected.
.ymd_to_date <- function(year, month, day) {
  if (inherits(year, c("data.frame", "matrix"))) {
    x <- year
    colnames(x) <- tolower(colnames(x))
    .require_columns(x, c("year", "month", "day"))

    year <- x[, "year"]
    month <- x[, "month"]
    day <- x[, "day"]

  } else if (inherits(year, "numeric")) {
    if(length(month) != length(year) || length(day) != length(year)) {
      stop("year, month and day vectors must be the same length")
    }
  }

  as.Date( sprintf("%4d-%02d-%02d", year, month, day) )
}


# Convert a vector of Date objects to a data frame of
# integer year, month, day values
.date_to_ymd <- function(dates) {
  stopifnot(inherits(dates, "Date"))

  data.frame(
    year = as.integer(lubridate::year(dates)),
    month = as.integer(lubridate::month(dates)),
    day = lubridate::day(dates)
  )
}


# Add records for any missing days in a time series data frame.
.add_missing_days <- function(dat) {
  .require_columns(dat, c("year", "month", "day", "hour", "minute"))

  # If a station column is present there must be only one value
  station <- NA
  station.col <- match("station", tolower(colnames(dat)))
  if (!is.na(station.col)) {
    station <- unique(dat[, station.col])
    if (length(station) != 1) {
      stop("Data records should be for just one station")
    }
  }

  dates <- unique(.ymd_to_date(dat$year, dat$month, dat$day))

  dates <- setdiff(seq(min(dates), max(dates), by = "1 day"),
                   dates)

  if (length(dates) > 0) {
    # setdiff gives a vector of days-since-epoch values instead
    # of Date objects, so fix that
    dates <- as.Date(dates, origin = "1970-01-01")

    extras <- .date_to_ymd(dates)
    extras$hour <- 0
    extras$minute <- 0

    if (!is.na(station)) extras$station <- station

    dat <- dplyr::bind_rows(dat, extras)
    attr(dat, "dates.added") <- dates

  } else {
    # No dates added. Set attribute to empty Date vector
    attr(dat, "dates.added") <- as.Date(x = integer(0), origin = "1970-01-01")
  }

  dat
}



# Given a vector, check for a block of one or more missing values at the tail
# and, if found, return the index for the first missing value in that block.
# Return NA if no such block is present.
.find_na_tail <- function(x) {
  N <- length(x)
  if (!is.na(x[N])) {
    NA
  } else {
    i <- match(FALSE, rev(is.na(x)), nomatch = N+1)
    N - i + 2
  }
}


# Guess the data type of a set of weather records in standard form
# by checking column names and, if necessary, time steps
#   'windspeed' AND 'windgust' => aws
#   sub-hourly time steps => aws
#   else => synoptic
#
.guess_data_type <- function(dat) {
  colnames(dat) <- tolower(colnames(dat))

  RequiredCols <- c("year", "month", "day", "hour", "minute")
  .require_columns(dat, RequiredCols)

  windcols <- c("windspeed", "windgust") %in% colnames(dat)
  if (all(windcols)) {
    "aws"
  } else {
    # check for sub-hourly time steps
    i <- dplyr::group_indices(dat, year, month, day, hour)
    if (dplyr::n_distinct(i) < length(i)) {
      "aws"
    } else {
      "synoptic"
    }
  }
}


# Check that expected column names are present in a data frame and
# stop with an error message if not.
.require_columns <- function(dat, expected.colnames, ignore.case = FALSE) {
  if (ignore.case) fn <- tolower
  else fn <- identity

  found <- fn(expected.colnames) %in% fn(colnames(dat))
  if (any(!found)) {
    stop("The following required columns are not present:\n",
         paste(expected.colnames[!found], collapse = ", "))
  }
}


# Convert NA values to zero
.na2zero <- function(x) {
  ifelse(is.na(x), 0, x)
}


# Version of max that returns NA instead of -Inf if x is
# empty or all values are NA
.max_with_na <- function(x) {
  # If all values are NA, return NA instead of -Inf
  x <- na.omit(x)
  if (length(x) == 0) NA
  else max(x, na.rm = TRUE)
}
