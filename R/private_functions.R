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
  Expected <- c("aws", "synoptic", "stations")
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


.is_connection <- function(db) {
  cl <- inherits(db, c("Pool", "R6"), which = TRUE)
  all(cl == c(1,2))
}


.is_open_connection <- function(db) {
  .is_connection(db) && pool::dbIsValid(db)
}


# An alternative to the base R file.path function that attempts to resolve
# the mix of single, double, forward and back slashes that can occur.
#
.safe_file_path <- function(...) {
  x <- file.path(...)
  x <- stringr::str_split(x, "[\\\\/]+")
  sapply(x, stringr::str_c, collapse = "/")
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
  x <- stats::na.omit(x)
  if (length(x) == 0) NA
  else max(x, na.rm = TRUE)
}
