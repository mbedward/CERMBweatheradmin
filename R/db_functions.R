#' Import BOM weather station data into a database
#'
#' This function can read delimited text data, in the format used by the Bureau
#' of Meteorology, from an individual weather station file or a directory or zip
#' file containing one or more such files, stations in CSV format, and import
#' them into a SQLite database.
#'
#' In the case of the input data source being a directory or zip file, weather
#' station files are identified by searching for names that include 'Data'
#' followed by digits and underscores, with the file extension '.txt'. Any
#' input records already present in the database are silently ignored.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param datapath Character path to one of the following: an individual weather
#'   station data file in CSV format; a directory containing one or more data
#'   files; or a zip file containing one or more such files. Zip files are
#'   assumed to have a '.zip' extension.
#'
#' @param stations Either NULL (default) to import data for all stations, or a
#'   character vector of station identifiers. Ignored if \code{datapath} is a
#'   single file.
#'
#' @param allow.missing If TRUE (default) and specific stations were requested,
#'   the function will silently ignore any that are missing in the directory or
#'   zip file. If FALSE, missing stations result in an error.
#'
#' @return \code{TRUE} if the process was completed, or \code{FALSE} otherwise.
#'   Note that completion of the process means only that the input data were
#'   successfully read, not necessarily that new records were added to the
#'   database.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Sometime earlier at start of session
#' DB <- bom_db_init("c:/foo/bar/weather.db", readonly = FALSE)
#'
#' # Import data from a BOM zip file containing individuals CSV-format
#' # data files for weather stations
#' bom_db_import(DB, "c:/bom/updates/some_aws.zip")
#'
#' # Import data from CSV-format files in a directory
#' bom_db_import(DB, "c:/bom/archive/aws_files/")
#'
#' # Import data from an individual CSV-format weather station file
#' bom_db_import(DB, "c:/foo/bar/HC06D_Data_068228_999999999515426.txt")
#'
#' # Do other things, then at end of session...
#' bom_db_close(DB)
#' }
#'
bom_db_import <- function(db,
                          datapath,
                          stations = NULL,
                          allow.missing = TRUE) {

  datapath <- stringr::str_trim(datapath)

  if (!(file.exists(datapath) || dir.exists(datapath)))
    stop("Cannot access ", datapath)

  .ensure_connection(db)

  # Initial record count for each table
  Nrecs.init <- bom_db_summary(db, by = "total")

  if (.is_zip_file(datapath))
    .do_import_zip(db, datapath, stations, allow.missing)

  else if (.is_directory(datapath))
    .do_import_dir(db, datapath, stations, allow.missing)

  else
    .do_import_file_via_pool(db, datapath)
}


# Helper function to create an SQL INSERT OR IGNORE statement
# for a given data frame. Assumes column names are standard and
# the data frame has a 'datatype' attribute corresponding to a
# table name.
#
.sql_import <- function(dat) {
  tblname <- attributes(dat)$datatype
  params <- paste(paste0(":", colnames(dat)), collapse = ", ")
  paste0("INSERT OR IGNORE INTO ", tblname, " VALUES (", params, ")")
}


# Helper function to import data from a zip file.
#
.do_import_zip <- function(db,
                           zipfile,
                           stations = NULL,
                           allow.missing = TRUE) {

  # Check for empty zip file
  info <- bom_zip_summary(zipfile)
  if (nrow(info) == 0) return(FALSE)

  if (is.null(stations)) {
    stations <- info[["station"]]
  }

  dats <- bom_zip_data(zipfile, stations, allow.missing)

  pool::poolWithTransaction(
    db,
    function(conn) {
      for (dat in dats) {
        rs <- DBI::dbSendStatement(conn, .sql_import(dat))
        DBI::dbBind(rs, params = dat)
        DBI::dbClearResult(rs)
      }
    }
  )
}


# Helper function to import data from a directory containing one
# or more weather station data files in CSV format.
#
.do_import_dir <- function(db,
                           dirpath,
                           stations = NULL,
                           allow.missing = TRUE) {

  info <- bom_dir_summary(dirpath)

  if (is.null(stations)) stations <- info[["station"]]

  ids <- bom_station_id(stations)
  info <- dplyr::filter(info, station %in% ids)

  if (nrow(info) > 0) {
    pool::poolWithTransaction(
      db,
      function(conn) {

        for (i in 1:nrow(info)) {
          if (info[[i, "filesize"]] > 0) {
            filepath <- .safe_file_path(dirpath, info[[i, "filename"]])
            .do_import_file_via_connection(conn, filepath)
          }
        }
      }
    )
  }
}


# Helper function to import an individual CSV-format data file.
# Uses a database connection pool object and wraps the insert in
# a transaction.
#
.do_import_file_via_pool <- function(dbpool, filepath) {
  pool::poolWithTransaction(
    dbpool,
    function(conn) .do_import_file_via_connection(conn, filepath)
  )
}


# Helper function to import an individual CSV-format data file.
# Uses a database connection object directly.
#
.do_import_file_via_connection <- function(conn, filepath) {
  dat <- try(read.csv(filepath, stringsAsFactors = FALSE), silent=TRUE)

  if (inherits(dat, "try-error")) FALSE
  else {
    dat <- .map_fields(dat)

    # Ensure we have integer or numeric data in each column
    # (some BOM data sets have empty values as character strings)
    coltypes <- attributes(dat)$coltypes
    stopifnot(length(coltypes) == ncol(dat))

    suppressWarnings(
      for (i in 1:ncol(dat)) {
        if (coltypes[i] == "integer") {
          dat[[i]] <- as.integer(dat[[i]])
        } else if (coltypes[i] == "numeric") {
          dat[[i]] <- as.numeric(dat[[i]])
        } else {
          # This would be a package programming error
          stop("Unknown column type in COLUMN_LOOKUP: ", coltypes[i])
        }
      }
    )

    rs <- DBI::dbSendStatement(conn, .sql_import(dat))
    DBI::dbBind(rs, params = dat)
    DBI::dbClearResult(rs)

    TRUE
  }
}


#' Create a new database for weather data
#'
#' This function creates a new database with tables for synoptic and AWS data.
#' SQLite databases consist of a single file which holds all tables. The file
#' extension is arbitrary and may be omitted, but using '.db' or '.sqlite' is
#' recommended for sanity.
#'
#' @param dbpath A character path to the new database file. An error is thrown
#'   if the file already exists.
#'
#' @return A database connection pool object that can be used with other package
#'   functions such as \code{\link{bom_db_import}} as well as with \code{dplyr}
#'   functions. It should be closed at the end of a session with
#'   \code{\link{bom_db_close}}.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Create a new database file with the required weather data tables
#' # for AWS and synoptic data
#' DB <- bom_db_create("c:/foo/bar/weather.db")
#'
#' # Do things with it
#' bom_db_import(DB, "c:/foo/bar/update_aws.zip")
#'
#' # At end of session
#' bom_db_close(DB)
#' }

bom_db_create <- function(dbpath) {
  if (file.exists(dbpath)) stop("File already exists: ", dbpath)

  DB <- pool::dbPool(RSQLite::SQLite(), dbname = dbpath, flags = RSQLite::SQLITE_RWC)

  conn <- pool::poolCheckout(DB)

  res <- DBI::dbSendQuery(conn, .BOM_SQL$create_synoptic_table)
  DBI::dbClearResult(res)

  res <- DBI::dbSendQuery(conn, .BOM_SQL$create_aws_table)
  DBI::dbClearResult(res)

  pool::poolReturn(conn)

  DB
}

#' Open a connection to an existing database
#'
#' This function connects an existing database and checks that it contains the
#' required tables for synoptic and AWS data. By default, it returns a read-only
#' connection.
#'
#' @param dbpath A character path to an existing database file.
#'
#' @param readonly If TRUE (default) a read-only connection is returned that
#'   you can use to query the database but not to import new data. If
#'   FALSE, a read-write connection is returned that can be used with
#'   \code{\link{bom_db_import}}.
#'
#' @return A database connection pool object that can be used with other package
#'   functions such as \code{\link{bom_db_import}} as well as with \code{dplyr}
#'   functions. It should be closed at the end of a session with
#'   \code{\link{bom_db_close}}.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Open a database
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # Do things with it
#' bom_db_import(DB, "c:/foo/updates/some_aws.zip")
#'
#' # At end of session
#' bom_db_close(DB)
#' }
#'
bom_db_open <- function(dbpath, readonly = TRUE) {
  if (!file.exists(dbpath)) stop("File not found: ", dbpath)

  if (dir.exists(dbpath)) stop("Expected a file not a directory: ", dbpath)

  if (readonly) flags <- RSQLite::SQLITE_RO
  else flags <- RSQLite::SQLITE_RW

  DB <- pool::dbPool(RSQLite::SQLite(), dbname = dbpath, flags = flags)

  .ensure_connection(DB)

  DB
}


#' Close a connection to a database
#'
#' Given a database connection pool object, as returned by
#' \code{\link{bom_db_create}} or \code{\link{bom_db_open}}, this function
#' checks whether the connection is open and, if so, closes it. After being
#' closed, the connection object can no longer be used.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @return Invisibly returns TRUE if the connection was closed, or FALSE
#'   otherwise (e.g. the connection had been closed previously).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # do things with the database, then...
#'
#' bom_db_close(DB)
#' }
#'
bom_db_close <- function(db) {
  res <- FALSE

  if (.is_open_connection(db)) {
    pool::poolClose(db)
    res <- TRUE
  }

  invisible(res)
}


#' Check if a database connection is read-only
#'
#' Checks if the given database connection is read-only, ie. can be used to
#' retrieve data but not import new data. An error is issued if \code{db}
#' is not a currently open connection to a weather database.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @return TRUE if the connection is read-only; or FALSE if it supports write
#'   operations.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # This will return TRUE
#' bom_db_readonly(DB)
#'
#' # Re-open connection with write permission
#' bom_db_close(DB)
#' DB <- bom_db_open("c:/foo/bar/weather.db", readonly = FALSE)
#'
#' # Now this will return FALSE
#' bom_db_readonly(DB)
#' }
#'
bom_db_readonly <- function(db) {
  .ensure_connection(db)

  con <- pool::poolCheckout(db)
  flags <- con@flags
  pool::poolReturn(con)

  bitwAnd(flags, RSQLite::SQLITE_RO) > 0
}


#' Check if a database connection supports read and write operations
#'
#' Checks if the given database connection can be used for write operations
#' (importing new data) as well as read operations (querying and retrieving
#' data). An error is issued if \code{db} is not a currently open connection to
#' a weather database.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @return TRUE if the connection supports write operations; or FALSE if it is
#'   read-only.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # This will return FALSE
#' bom_db_readwrite(DB)
#'
#' # Re-open connection with write permission
#' bom_db_close(DB)
#' DB <- bom_db_open("c:/foo/bar/weather.db", readonly = FALSE)
#'
#' # Now this will return TRUE
#' bom_db_readwrite(DB)
#' }
#'
bom_db_readwrite <- function(db) {
  !bom_db_readonly(db)
}


#' Gets a \code{tbl} object for AWS data to use with dplyr functions
#'
#' This function takes an open connection to a database and returns a dplyr
#' \code{tbl} object for AWS data to use with dplyr functions.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @return A \code{tbl} object representing the AWS table to use with dplyr.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to a database
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # Get a tbl object for AWS data
#' taws <- bom_db_aws(db)
#'
#' # Get the field names in the table
#' colnames(taws)
#'
#' # Use dplyr to find the maximum temperature recorded at each
#' # weather station
#'
#' library(dplyr)
#'
#' dat <- taws %>%
#'   group_by(station) %>%
#'   summarize(maxtemp = max(temperature, na.rm = TRUE)) %>%
#'
#'   # omit records for stations with no temperature values
#'   filter(!is.na(maxtemp)) %>%
#'
#'   # tell dplyr to execute this query on the database
#'   collect()
#' }
#'
bom_db_aws <- function(db) {
  .ensure_connection(db)
  dplyr::tbl(db, "AWS")
}


#' Gets a \code{tbl} object for synoptic data to use with dplyr functions
#'
#' This function takes an open connection to a database and returns a dplyr
#' \code{tbl} object for synoptic data to use with dplyr functions.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @return A \code{tbl} object representing the synoptic table to use with
#'   dplyr.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to a database
#' DB <- bom_db_open("c:/foo/bar/weather.db")
#'
#' # Get a tbl object for AWS data
#' tsynoptic <- bom_db_aws(db)
#'
#' # Get the field names in the table
#' colnames(tsynoptic)
#'
#' # Use dplyr to find the maximum temperature recorded at each
#' # weather station
#'
#' library(dplyr)
#'
#' dat <- tsynoptic %>%
#'   group_by(station) %>%
#'   summarize(maxtemp = max(temperature, na.rm = TRUE)) %>%
#'
#'   # omit records for stations with no temperature values
#'   filter(!is.na(maxtemp)) %>%
#'
#'   # tell dplyr to execute this query on the database
#'   collect()
#' }
#'
bom_db_synoptic <- function(db) {
  .ensure_connection(db)
  dplyr::tbl(db, "Synoptic")
}


#' Gets a summary of database contents
#'
#' Gets the count of database records in the AWS and Synoptic tables.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param by One of 'total' for total records per table, or 'station' for
#'   count of records by weather station.
#'
#' @param approx If TRUE (default) and \code{by == "total"} return an
#'   approximate record count based on the maximum RowID for each table. This is
#'   almost instant whereas a full record count is very slow (minutes rather
#'   than seconds) for large tables. However, it is approximate because RowID is
#'   an automatically incremented variable that does not account for any
#'   previously deleted records. Ignored when \code{by == "station"}.
#'
#' @return A data frame with columns: table; station (if argument 'by' was
#'   'station'); nrecs.
#'
#' @export
#'
bom_db_summary <- function(db, by = c("total", "station"), approx = TRUE) {
  by = match.arg(by)

  .ensure_connection(db)

  if (by == "station") {
    sqltxt <- "SELECT station, COUNT(*) AS nrecs FROM {`tbl`} GROUP BY station"
    empty <- data.frame(station = NA_integer_, nrecs = 0)
  }
  else { # by == "total"
    if (approx) sqltxt <- "SELECT MAX(ROWID) AS nrecs FROM {`tbl`}"
    else sqltxt <- "SELECT COUNT(*) AS nrecs FROM {`tbl`}"
    empty <- data.frame(nrecs = 0)
  }

  res <- lapply(c("AWS", "Synoptic"), function(tblname) {
    sql <- glue::glue_sql(sqltxt, .con=db, tbl=tblname)
    x <- DBI::dbGetQuery(db, sql)
    if ( nrow(x) == 0 || is.na(x[["nrecs"]]) ) x <- empty
    x[["table"]] <- tblname
    x
  })

  res <- dplyr::bind_rows(res)

  if (by == "station") res <- res[, c("table", "station", "nrecs")]
  else res <- res[, c("table", "nrecs")]

  res
}


#' Check that records for a given station form an uninterrupted time series
#'
#' Checks whether the records for each station form a non-interrupted time series.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param station Integer station identifier.
#'
#' @param dbtable Either 'AWS' or 'Synoptic'. Case-insensitive and may be
#'   abbreviated. Default is 'AWS'.
#'
#' @return A named list with elements:
#'   \describe{
#'     \item{ok}{TRUE if records form an uninterrupted time series; FALSE otherwise.}
#'     \item{err}{If 'ok' is FALSE, message indicating first break in time series.}
#'   }
#'
#' @export
#'
bom_db_check_datetimes <- function(db,
                                   station,
                                   dbtable = c("aws", "synoptic")) {

  .ensure_connection(db)

  # Case-insensitive match for table names
  dbtable <- match.arg(toupper(dbtable), c("AWS", "SYNOPTIC"), several.ok = FALSE)
  if (dbtable == "SYNOPTIC") dbtable <- "Synoptic"

  cmd <- glue::glue("select year, month, day, hour, minute from {dbtable}
                    where station = {station}
                    order by year, month, day, hour, minute;")

  dat <- pool::dbGetQuery(db, cmd)

  if (nrow(dat) == 0) {
    msg <- glue::glue("No records in table {dbtable} for station {station}")
    warning(msg, immediate. = TRUE)

    list(ok = TRUE, err = "")

  } else {
    check <- .do_check_datetimes(dat, daily = FALSE)
    list(ok = check$ok, err = check$err)
  }
}


# Check that data records form an uninterrupted series of days.
#
# dat - A data frame with columns year, month, day (and possibly others).
#
# daily - If TRUE, check that here is only one record per day.
#   A value must be supplied.
#
# Returns a list with elements:
#   ok - TRUE if data are valid and time series is uninterrupted.
#   rec.order - Integer vector giving date-time order of records
#     or NULL if ok is FALSE.
#
.do_check_datetimes <- function(dat, daily) {
  if (missing(daily)) stop("Argument 'daily' (logical) must be provided")

  colnames(dat) <- tolower(colnames(dat))
  if (!all(c("year", "month", "day") %in% colnames(dat))) {
    stop("Columns year, month, day are required")
  }

  # If less than two records, order doesn't matter
  if (nrow(dat) < 2) return(list(ok = TRUE, rec.order = seq_len(nrow(dat))))

  ok <- TRUE

  dat <- dat %>%
    # ungroup just in case
    dplyr::ungroup() %>%

    dplyr::mutate(.recindex = dplyr::row_number(),
                  date = sprintf("%4d-%02d-%02d", year, month, day),
                  date = lubridate::ymd(date))

  # If only daily records are expected, check this
  if (daily) {
    if (!(dplyr::n_distinct(dat$date) == nrow(dat))) {
      ok <- FALSE
      err <- "Expected only one record per day"
    }
  }

  if (ok) {
    # Order records
    ovars <- c("year", "month", "day")
    if ("hour" %in% colnames(dat)) ovars <- c(ovars, "hour")
    if ("minute" %in% colnames(dat)) ovars <- c(ovars, "minute")
    dat <- dplyr::arrange_at(dat, ovars)

    # Check that there are no missing days
    dat <- dat %>%
      dplyr::mutate(diff = as.integer(date - dplyr::lag(date)))

    # First record is ignored because there is no prior date
    okdiffs <- c(TRUE, dat$diff[-1] %in% 0:1)
    if (any(!okdiffs)) {
      ok <- FALSE
      i <- which(!okdiffs)[1]
      err <- glue::glue("Time series gap before {dat$date[i]}")
    }
  }

  if (ok) {
    list(ok = TRUE, err = NULL, rec.order = dat$.recindex)
  } else {
    list(ok = FALSE, err = err, rec.order = NULL)
  }
}


#' Format station identifying numbers as character strings
#'
#' Formats station numbers to match the embedded strings in station
#' data file names. Identifiers can be provided as integers or strings.
#' A valid station number has between four and six digits.
#'
#' @param id Vector (integer or character) of one or more station
#'   identifying numbers.
#'
#' @return Vector of formatted identifiers.
#'
#' @export
#'
bom_station_id <- function(id) {
  if (is.numeric(id)) {
    id <- sapply(id, function(x) sprintf("%06d", x))
  }
  else if (is.character(id)) {
    len <- stringr::str_length(id)
    if ( any(len > 6) ) stop("One or more station identifiers have more than 6 characters")

    id <- stringr::str_pad(id, width = 6, side = "left", pad = "0")
  }

  id
}

