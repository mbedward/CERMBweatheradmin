#' Import weather station data from a zip file
#'
#' Imports data from a zip file, containing data sets for one or more weather
#' stations in CSV format, into a SQLite database.
#'
#' @param zipfile Path to the input zip file.
#'
#' @param db Either an open database connection or the path to a database file to
#'   create or update.
#'
#' @param stations Either NULL (default) to import data for all stations from
#'   the zip file; or a character vector of station identifiers.
#'
#' @param allow.missing If TRUE (default) and specific stations were requested,
#'   the function will silently ignore any that are missing in the zip file. If
#'   FALSE, missing stations result in an error.
#'
#' @return Connection to the destination database.
#'
#' @export
bom_import <- function(zipfile,
                       db,
                       stations = NULL,
                       allow.missing = TRUE) {

  browser()

  con <- bom_db_init(db)

  if (is.null(stations)) stations <- bom_zip_info(zipfile)[["station"]]

  dats <- bom_station_data(zipfile, stations, allow.missing)

  for (dat in dats) {
    DBI::dbWriteTable(con, name = attributes(dat)$datatype, value = dat, append = TRUE)
  }

  con
}


#' Initializes a database with weather data tables as required
#'
#' This function can be used to create a new database with tables for
#' synoptic and time series data, or to check an existing database and
#' create the required tables if they are not already present.
#'
#' @param db Either an open connection to a SQLite database; a character
#'   path to an existing database to open; or a character path to a
#'   database to create (if argument \code{existing} is FALSE).
#'
#' @param existing If TRUE and \code{db} is a path string then an error will
#'   be thrown if the database file does not already exist. If FALSE (default),
#'   the file will be created if necessary. Ignored if \code{db} is a
#'   connection object rather than a path string.
#'
#' @return A connection (\code{SQLiteConnection} object) to the database.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Open a database file, or create it if it does not exist, and
#' # initialize the weather data tables as required.
#' con <- bom_db_init("c:/foo/bar/weather.db")
#'
#' # Open an existing database file. An error is thrown if it does
#' # not exist.
#' con <- bom_db_init("c:/foo/bar/weather.db", existing = TRUE)
#'
#' # Pass in an open connection to a database so that
#' # weather data tables will be created if not already present
#' con <- bom_db_init(con)
#' }
#'
bom_db_init <- function(db, existing = FALSE) {
  if (.is_connection(db)) {
    if (!DBI::dbIsValid(db)) stop("Supplied connection is not open")
    con <- db
  }
  else if (is.character(db)) {
    if (existing & !file.exists(db))
      stop("Database file not found: ", db)

    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db)
  }
  else {
    stop("Argument db should be an open database connection or a path string")
  }


  if (! .db_has_table(con, "Synoptic")) {
    res <- DBI::dbSendQuery(con, BOM_SQL$create_synoptic_table)
    DBI::dbClearResult(res)
  }

  if (! .db_has_table(con, "Aws")) {
    res <- DBI::dbSendQuery(con, BOM_SQL$create_aws_table)
    DBI::dbClearResult(res)
  }

  con
}


#' Gets a `tbl` object for use with dplyr functions
#'
#' @param db Either an open connection to a SQLite database; or a character
#'   path to an existing database to open.
#'
bom_db_tbl <- function(db, tblname = c("synoptic", "aws")) {
  tblname <- match.arg(tblname)
  con <- bom_db_init(db, exists = TRUE)

  t <- dplyr::tbl(con, tblname)
  DBI::dbDisconnect(con)

  t
}


#' Summarizes weather stations in a zipped data file
#'
#' This function summarizes the contents of a zip file containing data sets
#' for one or more weather stations.
#'
#' @param zipfile Path to the input zip file.
#'
#' @param include One of the following (may be abbreviated):
#'   'data' (default) to only list stations with non-empty data sets;
#'   'all' to list all stations;
#'   'empty' to only list stations with empty data sets.
#'
#' @return A data frame with columns station (identifier); filename; filesize.
#'
#' @importFrom dplyr %>%
#'
#' @export
#'
bom_zip_info <- function(zipfile, include = c("data", "all", "empty")) {
  include <- match.arg(include)

  files <- unzip(zipfile, list = TRUE)

  # Identify station data files
  pattern <- "_Data_[_\\d]+\\.txt$"
  ii <- stringr::str_detect(files[["Name"]], pattern)

  ii <- switch(include,
               data = ii & files[["Length"]] > 0,
               all = ii,
               empty = ii & files[["Length"]] == 0)

  # Data file names
  filename <- files[["Name"]][ii]

  # Station IDs
  station <- filename %>%
    stringr::str_extract("_Data_\\d+") %>%
    stringr::str_replace("_Data_", "")

  data.frame(station, filename, filesize = files[["Length"]][ii],
             stringsAsFactors = FALSE)
}


#' Reads raw data for specified weather stations from a zip file
#'
#' @param zipfile Path to the input zip file.
#'
#' @param station Station identifiers as integer station numbers or character strings.
#'
#' @param allow.missing If TRUE (default), the function will return NULL if the
#'   requested station is not found in the zip file. If FALSE, an error
#'   is thrown if the station is missing.
#'
#' @param out.format One of 'list' (default) to return a named list of data frames,
#'   or 'single' to return a single frame of data for all stations.
#'
#' @return If out.format is 'list', a named list, where names are station
#'   identifiers (character strings of length six) and elements are data frames
#'   of station data. Each data frame has an attribute 'datatype' with value 'synoptic'
#'   or 'aws'. If out.format is 'single', a combined data frame of data for all
#'   stations, with an attribute 'datatype'.
#'
#' @export
#'
bom_station_data <- function(zipfile, stations,
                             allow.missing = TRUE,
                             out.format = c("list", "single")) {

  out.format <- match.arg(out.format)

  stn.info <- bom_zip_info(zipfile)

  id <- bom_station_id(stations)

  ii <- match(id, stn.info[["station"]])
  if (anyNA(ii)) {
    if (allow.missing) {
      if (all(is.na(ii)))
        return(NULL)  # TODO better option?
      else
        ii <- na.omit(ii)

    } else {
      # missing stations not allowed
      misses <- stations[is.na(ii)]
      stop("No data found for the following station(s)\n", misses)
    }
  }

  filenames <- stn.info[["filename"]][ii]

  res <- lapply(filenames,
                function(fname) {
                  zcon <- unz(zipfile, fname)
                  dat <- read.csv(zcon, stringsAsFactors = FALSE)
                  .map_fields(dat)
                })

  if (out.format == "list") {
    names(res) <- stn.info[["station"]][ii]
  }
  else { # out.format == "single"
    # Note: we assume all data is of the same type, synoptic or AWS
    type <- attr(res[[1]], "datatype")
    res <- dplyr::bind_rows(res)
    attr(res, "datatype") <- type
  }

  res
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


###### Private (non exported) functions

.is_connection <- function(x) inherits(x, "SQLiteConnection")

.is_open_connection <- function(x) .is_connection(x) && RSQLite::dbIsValid(x)


# Selects and renames columns in a data frame of raw data
.map_fields <- function(dat) {
  type <- .get_data_type(dat)

  lookup <- dplyr::filter(COLUMN_LOOKUP, datatype == type)
  ii <- match(colnames(dat), lookup[["input"]])
  if (anyNA(ii)) stop("Unrecognized column name(s): ", colnames(dat)[is.na(ii)])

  dbnames <- lookup[["db"]][ii]
  colnames(dat) <- dbnames

  dat <- dat[, !is.na(dbnames)]
  attr(dat, "datatype") <- type

  dat
}


.get_data_type <- function(dat.raw) {
  if (any(stringr::str_detect(colnames(dat.raw), "precipitation.*since.*9"))) "aws"
  else "synoptic"
}


.db_has_table <- function(con, tblname) {
  if (!.is_open_connection(con))
    stop("Database connection is not open")

  tolower(tblname) %in% tolower(DBI::dbListTables(con))
}


