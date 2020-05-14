#' Import BOM weather station data into a database
#'
#' This function can read delimited text data, in the format used by the Bureau
#' of Meteorology, from an individual weather station file or a directory or zip
#' file containing one or more such files, stations in CSV format, and import
#' them into a SQLite database. In the case of the input data source being a
#' directory or zip file, weather station files are identified by searching for
#' names that include 'Data' followed by digits and underscores, with the file
#' extension '.txt'. Any input records already present in the database are
#' silently ignored. Note that fire-related variables (KBDI, drought factor and
#' FFDI) are \strong{not} calculated for the new records. call the function
#' \code{bom_db_update_fire} to do this.
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param datapath Character path to one of the following: an individual weather
#'   station data file in CSV format; a directory containing one or more data
#'   files; or a zip file containing one or more such files. If a zip file
#'   (identified by a '.zip' extension) the path should be to a single file.
#'
#' @param stations Either NULL (default) to import data for all stations, or a
#'   character vector of station identifiers. Ignored if \code{datapath} is a
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
#' @seealso \code{\link{bom_db_update_fire}}
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
#' @export
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

  varnames <- paste(colnames(dat), collapse = ", ")
  params <- paste(paste0(":", colnames(dat)), collapse = ", ")

  cmd <- glue::glue("INSERT OR IGNORE INTO {tblname}
                    ({varnames})
                    VALUES ({params});")
  cmd
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
#' This function creates a new database with tables: 'Synoptic' for synoptic
#' data records; 'AWS' for automatic weather station data records; and
#' 'Stations' with details of station names and locations. SQLite databases
#' consist of a single file which holds all tables. The file extension is
#' arbitrary and may be omitted, but using '.db' or '.sqlite' is recommended for
#' sanity.
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
#' # for AWS and synoptic data and weather station metadata
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

  pool::poolWithTransaction(DB, function(conn) {
    DBI::dbExecute(conn, SQL_CREATE_TABLES$create_synoptic_table)
    DBI::dbExecute(conn, SQL_CREATE_TABLES$create_aws_table)
    DBI::dbExecute(conn, SQL_CREATE_TABLES$create_stations_table)

    DBI::dbWriteTable(conn, "Stations", CERMBweather::STATION_METADATA, append = TRUE)
  })

  DB
}

#' Open a connection to an existing database
#'
#' This function connects an existing database and checks that it contains the
#' required tables for synoptic and AWS data. If a 'Stations' table is not present
#' in the database, it is added. By default, a read-only connection is returned.
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

  # Initially open as read-write for checking tables
  DB <- pool::dbPool(RSQLite::SQLite(), dbname = dbpath, flags = RSQLite::SQLITE_RW)
  .ensure_connection(DB)
  .ensure_stations_table(DB)

  if (readonly) {
    bom_db_close(DB)
    DB <- pool::dbPool(RSQLite::SQLite(), dbname = dbpath, flags = RSQLite::SQLITE_RO)
  }

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


#' Check that records in a data set form an uninterrupted time series
#'
#' Given a data frame of daily or sub-daily records for one or more weather
#' stations, this function checks whether there are any gaps in the time series.
#' A gap is defined as one or more missing days. The input data records can be
#' in any order. It is primarily a helper function, called by other functions in
#' the package that require an uninterrupted time series, but can also be used
#' directly.
#'
#' @param dat A data frame with date columns (integer year, month and day),
#'   optional time columns (integer hour and minute) and possibly other
#'   variables. Normally a column of integer station identifiers will be present
#'   but this is optional. If missing, it will be assumed that all records pertain to
#'   a single weather station.
#'
#' @param daily If \code{TRUE}, expect only one record per day and return a
#'   failed check result if this is not the case. Note that a value must be
#'   supplied for this argument.
#'
#' @return A nested list with one element per station, where each element is a
#'   list consisting of:
#'   \describe{
#'     \item{station}{Integer station identifier (Set to -1 if no station
#'       column is provided).}
#'     \item{ok}{Logical value indicating success or failure of checks.}
#'     \item{gaps}{Dates (as Date objects) before which each gap in the time
#'       series occurs.}
#'   }
#'
#' @export
#'
bom_db_check_datetimes <- function(dat, daily) {
  if (missing(daily)) stop("Argument 'daily' (logical) must be provided")

  colnames(dat) <- tolower(colnames(dat))
  if (!all(c("year", "month", "day") %in% colnames(dat))) {
    stop("Columns year, month, day are required")
  }

  HasStation <- ("station" %in% colnames(dat))

  if (HasStation) {
    if (anyNA(dat$station)) stop("station column should not contain missing values")
  } else {
    dat$station <- -1
  }

  # ungroup just in case
  dat <- dplyr::ungroup(dat)

  # Vars to use for ordering records
  ovars <- c("year", "month", "day")
  if ("hour" %in% colnames(dat)) ovars <- c(ovars, "hour")
  if ("minute" %in% colnames(dat)) ovars <- c(ovars, "minute")

  # Run check for each station
  checks <- lapply(unique(dat$station), function(stn) {
    dat.stn <- dat %>% dplyr::filter(station == stn)

    # Default check value
    res <- list(station = stn,
                ok = TRUE,
                err = NULL,
                gaps = NULL)

    # If less than two records, gaps and order do not apply
    if (nrow(dat.stn) < 2) {
      return(res)
    }

    dat.stn <- dat.stn %>%
      dplyr::mutate(date = .ymd_to_date(year, month, day))

    # If only daily records are expected, check and return
    # early if that is not the case
    if (daily && dplyr::n_distinct(dat.stn$date) < nrow(dat.stn)) {
      res$ok <- FALSE
      res$err <- "Expected only one record per day"
      return(res)
    }

    dat.stn <- dplyr::arrange_at(dat.stn, ovars)

    # Check that there are no missing days
    dat.stn <- dat.stn %>%
      dplyr::mutate(diff = as.integer(date - dplyr::lag(date)))

    # First record is ignored because there is no prior date
    okdiffs <- c(TRUE, dat.stn$diff[-1] %in% 0:1)
    if (any(!okdiffs)) {
      res$ok <- FALSE
      res$err <- "Gap(s) in time series"
      res$gaps <- dat.stn$date[!okdiffs]
    }

    res
  })

  checks
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

