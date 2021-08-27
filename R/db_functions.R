#' Import BOM weather station data into a database
#'
#' This function can read delimited text data, in the format used by the Bureau
#' of Meteorology, from an individual weather station file, or from a directory
#' or zip file containing one or more such files in CSV format, and import them
#' into a connected database. In the case of the input data source being a
#' directory or zip file, weather station files are identified by searching for
#' names that include 'Data' followed by digits and underscores, with the file
#' extension '.txt'. Any records that are duplicate those already in the
#' database are silently ignored. Note that fire-related variables (KBDI,
#' drought factor and FFDI) are \strong{not} calculated for the new records.
#' \strong{TODO: link to the CERMBffdi package}
#'
#' @param db A database connection object created with \code{pool::dbPool} or
#'   \code{DBI::dbConnect}.
#'
#' @param datapath Character path to one of the following: an individual weather
#'   station data file in CSV format; a directory containing one or more data
#'   files; or a zip file containing one or more such files. If a zip file
#'   (identified by a '.zip' extension) the path should be to a single file.
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
#' @examples
#' \dontrun{
#' # Connect to a PostgreSQL database using an admin user name and password
#' DB <- DBI::dbConnect(drv = RPostgres::Postgres(),
#'                      host = "some.host.uow.edu.au",
#'                      dbname = "cermb_weather",
#'                      user = "admin_user_name",
#'                      password = "admin_password")
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
#' DBI::dbDisconnect(DB)
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

  if (CERMBweather::bom_is_zip_file(datapath))
    .do_import_zip(db, datapath, stations, allow.missing)

  else if (file.info(datapath)$isdir)
    .do_import_dir(db, datapath, stations, allow.missing)

  else
    .do_import_file(db, datapath)
}


.do_postgresql_import <- function(db, dat) {
  stopifnot(.get_db_type(db) == "postgresql")

  maintbl.name <- attr(dat, "datatype", exact = TRUE)
  stopifnot(maintbl.name %in% c("aws", "synoptic", "upperair"))

  temptbl.name <- paste0(maintbl.name, "_temp")

  sql.create_temp_table <- glue::glue(
    "CREATE TEMPORARY TABLE {temptbl.name}
     ON COMMIT DROP
     AS
     SELECT * FROM {maintbl.name}
     WITH NO DATA;")

  varnames <- colnames(dat)
  ii <- grepl("date_[a-z]+", varnames, ignore.case = TRUE)
  if (!sum(ii) == 2) stop("Expected two date columns")

  temp.varnames <- varnames

  # Need a type cast to transfer the text dates from the
  # temporary table into the date column of the main table
  temp.varnames[ii] <- paste0(temp.varnames[ii], "::date")

  sql.insert_recs <- glue::glue(
    "INSERT INTO {maintbl.name} (
      {paste(varnames, collapse = ', ')}
    )
    (SELECT {paste(temp.varnames, collapse = ', ')}
     FROM {temptbl.name})
    ON CONFLICT DO NOTHING;"
  )

  sql.drop_temp_table <- glue::glue(
    "DROP TABLE {temptbl.name};"
  )


  if (.is_pool_connection(db)) {
    pool::poolWithTransaction(db, function(conn) {
      DBI::dbExecute(conn, sql.create_temp_table)

      DBI::dbWriteTable(conn, temptbl.name, dat, overwrite = TRUE)
      DBI::dbExecute(conn, sql.insert_recs)

      DBI::dbExecute(conn, sql.drop_temp_table)
    })

  } else if (.is_dbi_connection(db)) {
    DBI::dbWithTransaction(db, {
      DBI::dbExecute(db, sql.create_temp_table)

      DBI::dbWriteTable(db, temptbl.name, dat, overwrite = TRUE)
      DBI::dbExecute(db, sql.insert_recs)

      DBI::dbExecute(db, sql.drop_temp_table)
    })
  }
}


# Helper function to import data from a zip file.
#
.do_import_zip <- function(db,
                           zipfile,
                           stations = NULL,
                           allow.missing = TRUE) {

  dbtype <- .get_db_type(db)
  if (dbtype == "sqlite") {
    stop("SQLite support has been removed (for now)")
  }

  .ensure_connection(db)

  # Check for empty zip file
  info <- CERMBweather::bom_zip_summary(zipfile)
  if (nrow(info) == 0) return(FALSE)

  if (is.null(stations)) {
    stations <- info[["station"]]
  }

  dats <- CERMBweather::bom_zip_data(zipfile, stations, allow.missing)

  for (dat in dats) {
    .do_postgresql_import(db, dat)
  }
}


# Helper function to import data from a directory containing one
# or more weather station data files in CSV format.
#
.do_import_dir <- function(db,
                           dirpath,
                           stations = NULL,
                           allow.missing = TRUE) {

  info <- CERMBweather::bom_dir_summary(dirpath)

  if (!is.null(stations)) {
    ids <- CERMBweather::bom_station_id(stations)
    info <- dplyr::filter(info, station %in% ids)
  }

  if (nrow(info) > 0) {
    for (i in 1:nrow(info)) {
      if (info[[i, "filesize"]] > 0) {
        # Use fs::path rather than base::file.path to ensure that
        # consistent forward slashes are used as path separators
        # regardless of input
        filepath <- fs::path(dirpath, info[[i, "filename"]])
        .do_import_file(db, filepath)
      }
    }
  }
}


# Helper function to import an individual CSV-format data file.
#
.do_import_file <- function(db, filepath) {
  dbtype <- .get_db_type(db)
  if (dbtype == "sqlite") {
    stop("SQLite support has been removed (for now)")
  }

  dat <- tryCatch(
    utils::read.csv(filepath, stringsAsFactors = FALSE),
    warning = function(w) NULL)

  if (is.null(dat)) {
    FALSE
  } else {
    dat <- CERMBweather::bom_tidy_data(dat)
    .do_postgresql_import(db, dat)
    TRUE
  }
}


#' Get count of records.
#'
#' Gets the count of records in the AWS and Synoptic tables. It can take forever
#' to get an exact count of records, especially from a PostgreSQL database.
#' Setting the \code{approx} argument to \code{TRUE} (default) will return a
#' very fast, approximate total count. This argument is ignored if the function
#' is called with \code{by = "station"}.
#'
#' @param db A database connection object created with \code{pool::dbPool} or
#'   \code{DBI::dbConnect}.
#'
#' @param by One of 'total' for total records per table, or 'station' for
#'   count of records by weather station.
#'
#' @param approx If TRUE (default) and \code{by == "total"} return an
#'   approximate record count. This is almost instant whereas a full record
#'   count is very slow (minutes rather than seconds) for large tables. However,
#'   it is only approximate and might not reflect recent additions or deletions.
#'   Ignored when \code{by == "station"}.
#'
#' @return A data frame with columns: table; station (if argument 'by' was
#'   'station'); nrecs.
#'
#' @export
#'
bom_db_record_count <- function(db, by = c("total", "station"), approx = TRUE) {
  by = match.arg(by)

  dbtype <- .get_db_type(db)
  if (dbtype == "sqlite") {
    stop("SQLite support has been removed (for now)")
  }

  .ensure_connection(db)

  if (by == "station") {
    command <- "SELECT station, COUNT(*) AS nrecs FROM '{tbl}' GROUP BY station"
    empty <- data.frame(station = NA_integer_, nrecs = 0)

  } else { # by == "total"
    if (approx) {
      command <-
        "SELECT reltuples::BIGINT AS nrecs FROM pg_class WHERE relname='public.{tbl}';"
    } else {
      command <- "SELECT COUNT(*) AS nrecs FROM public.{tbl}"
    }
    empty <- data.frame(nrecs = 0)
  }

  res <- lapply(c("aws", "synoptic"), function(tblname) {
    command <- glue::glue(command, tbl=tblname)
    x <- DBI::dbGetQuery(db, command)
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
      dplyr::mutate(date = CERMBweather::ymd_to_date(year, month, day))

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

