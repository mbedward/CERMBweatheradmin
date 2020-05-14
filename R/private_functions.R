###### Private (non exported) functions and variables

# Checks that an object is a valid database connection (pool object) and
# that the expected weather data tables are present
.ensure_connection <- function(db) {
  if (!.is_connection(db)) stop("Object is not a database connection pool")

  if (!pool::dbIsValid(db))
    stop("Database connection pool has been closed or was not initialized properly")

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


# Selects and renames columns in a data frame of raw data.
# Adds data type (aws or synoptic) and column types as attributes
# of the returned data frame.
.map_fields <- function(dat) {
  type <- .get_bomdata_type(dat)

  lookup <- dplyr::filter(COLUMN_LOOKUP, datatype == type)
  cnames <- tolower(colnames(dat))
  lookupnames <- tolower(lookup[["input"]])
  ii <- match(cnames, lookupnames)
  if (anyNA(ii)) stop("Unrecognized column name(s): ", colnames(dat)[is.na(ii)])

  dbnames <- lookup[["db"]][ii]
  ii.keep <- !is.na(dbnames)

  dat <- dat[, ii.keep]
  colnames(dat) <- dbnames[ii.keep]
  attr(dat, "datatype") <- type
  attr(dat, "coltypes") <- lookup[["coltype"]][ii.keep]

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


# Convert integer year, month, day values to Date objects
.ymd_to_date <- function(year, month, day) {
  if(length(month) != length(year) || length(day) != length(year)) {
    stop("year, month and day vectors must be the same length")
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
