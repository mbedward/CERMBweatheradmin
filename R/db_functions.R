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
#'
#' @param con An open database connection as returned by \code{\link{bom_db_init}}.
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
#' @param progress If TRUE display a progress bar during the import.
#'   If FALSE (default), do not display a progress bar.
#'
#' @return A named vector giving the number of rows added to the AWS and Synoptic
#'   tables.
#'
#' @export
#'
bom_db_import <- function(con,
                          datapath,
                          stations = NULL,
                          allow.missing = TRUE,
                          progress = FALSE) {

  datapath <- stringr::str_trim(datapath)

  if (!(file.exists(datapath) || dir.exists(datapath)))
    stop("Cannot access ", datapath)

  con <- bom_db_init(con)

  # Initial record count for each table
  Nrecs.init <- bom_db_summary(con, by = "total")

  if (.is_zip_file(datapath))
    .do_import_zip(con, datapath, stations, allow.missing, progress)

  else if (.is_directory(datapath))
    .do_import_dir(con, datapath, stations, allow.missing, progress)

  else
    .do_import_file(con, datapath)


  # Return a vector of number of records added to each table
  Nrecs <- bom_db_summary(con, by = "total")

  x <- Nrecs[["nrecs"]] - Nrecs.init[["nrecs"]]
  names(x) <- Nrecs[["table"]]

  x
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
.do_import_zip <- function(con,
                           zipfile,
                           stations = NULL,
                           allow.missing = TRUE,
                           progress = FALSE) {

  if (is.null(stations)) stations <- bom_zip_summary(zipfile)[["station"]]

  dats <- bom_zip_data(zipfile, stations, allow.missing)

  if (progress) pb <- txtProgressBar(max = length(dats), style = 3)
  k <- 0
  DBI::dbBegin(con)
  for (dat in dats) {
    rs <- DBI::dbSendStatement(con, .sql_import(dat))
    DBI::dbBind(rs, params = dat)
    DBI::dbClearResult(rs)
    k <- k + 1

    if (progress) setTxtProgressBar(pb, k)
  }
  DBI::dbCommit(con)

  if (progress) close(pb)
}


# Helper function to import data from a directory containing one
# or more weather station data files in CSV format.
#
.do_import_dir <- function(con,
                           dirpath,
                           stations = NULL,
                           allow.missing = TRUE,
                           progress = FALSE) {

  info <- bom_dir_summary(dirpath)

  if (is.null(stations)) stations <- info[["station"]]

  ids <- bom_station_id(stations)
  info <- dplyr::filter(info, station %in% ids)

  if (nrow(info) > 0) {

    if (progress) pb <- txtProgressBar(max = nrow(info), style = 3)
    k <- 0

    DBI::dbBegin(con)

    for (i in 1:nrow(info)) {
      if (info[[i, "filesize"]] > 0) {
        filepath <- .safe_file_path(dirpath, info[[i, "filename"]])
        .do_import_file(con, filepath)
      }
      k <- k + 1
      if (progress) setTxtProgressBar(pb, k)
    }

    DBI::dbCommit(con)

    if (progress) close(pb)
  }
}


# Helper function to import an individual CSV-format data file.
#
.do_import_file <- function(con, filepath) {
  dat <- read.csv(filepath, stringsAsFactors = FALSE)
  dat <- .map_fields(dat)

  rs <- DBI::dbSendStatement(con, .sql_import(dat))
  DBI::dbBind(rs, params = dat)
  DBI::dbClearResult(rs)
}


#' Opens or initializes a database for weather data
#'
#' This function can be used to open an existing database or create a new
#' database with tables for synoptic and time series data. In the case of an
#' existing database, the function will create tables for synoptic and AWS data
#' if they do not already exist.
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
#' @param copy If TRUE and \code{db} is an open database connection, the function
#'   returns a copy of the connection. If FALSE, the returned object will be
#'   the same as the input connection. Ignored if \code{db} is a path string.
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
bom_db_init <- function(db, existing = FALSE, copy = FALSE) {
  if (.is_connection(db)) {
    if (!DBI::dbIsValid(db)) stop("Supplied connection is not open")

    if (copy) con <- DBI::dbConnect(RSQLite::SQLite(), dbname = con@dbname)
    else con <- db
  }
  else if (is.character(db)) {
    if (existing & !file.exists(db))
      stop("Database file not found: ", db)

    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db)
    copy <- FALSE
  }
  else {
    stop("Argument db should be an open database connection or a path string")
  }


  if (! .db_has_table(con, "Synoptic")) {
    res <- DBI::dbSendQuery(con, .BOM_SQL$create_synoptic_table)
    DBI::dbClearResult(res)
  }

  if (! .db_has_table(con, "Aws")) {
    res <- DBI::dbSendQuery(con, .BOM_SQL$create_aws_table)
    DBI::dbClearResult(res)
  }

  con
}


#' Close a database connection
#'
#' Given a database connection object, this function checks whether the
#' connection is open and, if so, closes it. After being closed, the connection
#' object can no longer be used.
#'
#' This function is just a convenient short-cut for \code{\link[DBI]{dbIsValid}}
#' and \code{\link[DBI]{dbDisconnect}}.
#'
#' @param con A database connection
#'
#' @return Invisibly returns TRUE if the connection was closed, or FALSE
#'   otherwise (e.g. the connection had been closed previously).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' con <- bom_db_init("c:/foo/bar/weather.db")
#'
#' # do things with the database, then...
#'
#' bom_db_close(con)
#' }
#'
bom_db_close <- function(con) {
  if (.is_open_connection(con)) DBI::dbDisconnect(con)
  else invisible(FALSE)
}


#' Gets a \code{tbl} object for use with dplyr functions
#'
#' This function takes a connection to an open database, along with a table
#' name ('AWS' or 'Synoptic') and returns a dplyr \code{tbl} object for the
#' table that you can use with dplyr functions as if it were a data frame.
#'
#' @param con An open database connection as returned by
#'   \code{\link{bom_db_init}}.
#'
#' @param tblname One of 'Synoptic' or 'AWS'. May be abbreviated and case is
#'   ignored.
#'
#' @return A \code{tbl} object representing the table to use with dplyr.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to a database
#' con <- bom_db_init("c:/path/to/my/database.db")
#'
#' # Get a tbl object to use as a proxy for a chosen database table
#' tsynoptic <- bom_db_tbl(con, "syn")
#'
#' # Field names in the table
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
bom_db_tbl <- function(con, tblname) {
  .ensure_connection(con, .CON_FLAGS$Open, .CON_FLAGS$HasTables)

  if (missing(tblname)) stop("Argument tblname must be supplied")

  tblname <- match.arg(tolower(tblname), c("synoptic", "aws"))

  dplyr::tbl(con, tblname)
}


#' Gets a summary of database contents
#'
#' Gets the count of database records in the AWS and Synoptic tables.
#'
#' @param con An open database connection as returned by \code{\link{bom_db_init}}.
#'
#' @param by One of 'total' for total records per table, or 'station' for
#'   count of records by weather station.
#'
#' @return A data frame with columns: table; station (if argument 'by' was
#'   'station'); nrecs.
#'
#' @export
#'
bom_db_summary <- function(con, by = c("total", "station")) {
  by = match.arg(by)

  con <- bom_db_init(con, existing = TRUE, copy = TRUE)

  if (by == "station") {
    sqltxt <- "select station, count(*) as nrecs from <<tbl>> group by station"
    empty <- data.frame(station = NA_integer_, nrecs = 0)
  }
  else {
    sqltxt <- "select count(*) as nrecs from <<tbl>>"
    empty <- data.frame(nrecs = 0)
  }

  res <- lapply(c("AWS", "Synoptic"), function(tblname) {
    x <- DBI::dbGetQuery(con, stringr::str_replace(sqltxt, "<<tbl>>", tblname))
    if (nrow(x) == 0) x <- empty
    x[["table"]] <- tblname
    x
  })

  res <- dplyr::bind_rows(res)

  if (by == "station") res <- res[, c("table", "station", "nrecs")]
  else res <- res[, c("table", "nrecs")]

  bom_db_close(con)
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

