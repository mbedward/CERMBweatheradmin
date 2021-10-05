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
#' db <- DBI::dbConnect(drv = RPostgres::Postgres(),
#'                      host = "some.host.uow.edu.au",
#'                      dbname = "cermb_weather",
#'                      user = "admin_user_name",
#'                      password = "admin_password")
#'
#' # Import data from a BOM zip file containing individuals CSV-format
#' # data files for weather stations
#' bom_db_import(db, "c:/bom/updates/some_aws.zip")
#'
#' # Import data from CSV-format files in a directory
#' bom_db_import(db, "c:/bom/archive/aws_files/")
#'
#' # Import data from an individual CSV-format weather station file
#' bom_db_import(db, "c:/foo/bar/HC06D_Data_068228_999999999515426.txt")
#'
#' # Do other things, then at end of session...
#' DBI::dbDisconnect(db)
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


#' Add values for KBDI, Drought Factor and FFDI to database records
#'
#' This function uses the \code{CERMBffdi} package to calculate values for the
#' fire related variables: KBDI, drought factor and FFDI for selected weather
#' records, and store the values in the corresponding database columns. With
#' default arguments, the function will process all weather stations in the
#' 'aws' and 'synoptic' tables that were active on or after 2000-01-01. The
#' function attempts to minimize data transfer over the network and running time
#' by only processing the most recent records that do not already have FFDI
#' values. Set the argument \code{recalculate=TRUE} to override this behaviour
#' and force recalculation for all records.
#'
#' @param db A database connection object created with \code{pool::dbPool} or
#'   \code{DBI::dbConnect}.
#'
#' @param tables The tables to process. One or both of \code{'aws'} and \code{'synoptic'}.
#'   Default is both.
#'
#' @param states Standard abbreviations for the states to process. Default is
#'   \code{c('NSW', 'VIC', 'ACT', 'TAS', 'QLD', 'SA', 'WA')}.
#'
#' @param cutoff Cutoff date to subset stations to process. Values will only be
#'   calculated for stations that were active (i.e. have records in the
#'   database) on or after this date. Can be specified as a \code{Date} object
#'   or a character string in \code{'yyyy-mm-dd'} format. Default is
#'   \code{'2000-01-01'}.
#'
#' @param recalculate If \code{TRUE}, values will be calculated for all records and any existing values
#'   will be over-written. If \code{FALSE} (default), values will only be calculated for the most recent
#'   records that do not already have FFDI values.
#'
#' @export
#'
bom_db_add_ffdi <- function(db,
                            tables = c("synoptic", "aws"),
                            states = c("NSW", "VIC", "ACT", "TAS", "QLD", "SA", "WA"),
                            cutoff = as.Date("2000-01-01"),
                            recalculate = FALSE) {

  tables <- match.arg(tolower(tables),
                      choices = c("synoptic", "aws"),
                      several.ok = TRUE)

  states <- match.arg(toupper(states),
                      choices = c("NSW", "VIC", "ACT", "TAS", "QLD", "SA", "WA"),
                      several.ok = TRUE)

  if (!length(cutoff == 1)) {
    stop("cutoff should be a single Date object or character string in date format")
  }

  x <- inherits(cutoff, c("Date", "character"), which = TRUE)
  if (sum(x) == 0) {
    stop("cutoff should be a Date or a character string in date format")
  } else if (x[2] == 1) {
    cutoff <- as.Date(cutoff)
  }


  # Check that the database connection is valid
  ok <- CERMBweather::bom_db_is_connected(db)
  if (!ok) stop("Bummer - something wrong with database connection")


  # Helper function to recode missing quality flag values as 'X'
  quality_na2x <- function(x) {
    x <- toupper(x)
    ifelse(grepl("[YNWSI]", x), x, "X")
  }


  for (the.table in tables) {

    # Query the stations in the specified table and state(s) that
    # have data for the cutoff date and/or later.
    #
    for (the.state in states) {
      msg <- glue::glue("Processing {the.state} {the.table} stations")
      cat(msg, "\n")

      cmd <- glue::glue(
        "SELECT s.station, s.annualprecip_narclim, sub.most_recent
       FROM stations s
       INNER JOIN LATERAL (
         SELECT station, date_local AS most_recent
         FROM {the.table} t
         WHERE t.station = s.station
         ORDER BY date_local DESC
         LIMIT 1) sub
       ON TRUE

       WHERE s.state = '{the.state}' AND
         most_recent IS NOT NULL AND
         annualprecip_narclim IS NOT NULL AND
         most_recent >= '{cutoff}'::date
       ORDER BY s.station;")

      selected.stations <- DBI::dbGetQuery(db, cmd)

      if (nrow(selected.stations) > 0) {
        # --- Process each of the stations ---
        #
        pb <- progress::progress_bar$new(total = nrow(selected.stations),
                                         format = "[:bar] :current of :total - :percent :eta")

        for (istn in seq_len(nrow(selected.stations))) {
          the.station <- selected.stations$station[istn]
          the.avrain <- selected.stations$annualprecip_narclim[istn]
          the.latest_date <- selected.stations$most_recent[istn]

          pb$tick()

          # Find the date range for existing FFDI values for this station
          cmd <- glue::glue("SELECT station, first_ffdi_date, last_ffdi_date
                           FROM {the.table}_ffdi_dates
                           WHERE station = {the.station};")

          dat_existing_dates <- DBI::dbGetQuery(db, cmd)

          # Check whether we need to do this station
          if (recalculate ||
              nrow(dat_existing_dates) == 0 ||
              dat_existing_dates$last_ffdi_date < the.latest_date) {

            # Get station data. If not recalculating for all records,
            # just get the data for last 90 days to avoid wasting time
            # transferring a large data set over the network, but still
            # have enough wiggle room for KBDI etc.
            #
            if (recalculate) {
              date0 <- as.Date("1800-01-01")

            } else {
              date0 <- as.Date(the.latest_date) - 90
            }

            cmd <- glue::glue("SELECT * FROM {the.table}
                             WHERE station = {the.station} AND
                               date_local >= '{date0}'::date
                             ORDER BY date_local, hour_local, min_local;")

            dat <- DBI::dbGetQuery(db, cmd)
            Ndata_recs <- nrow(dat)

            if (Ndata_recs == 0) {
              # This should never happen because the query for selected stations
              # is based on the data table - so if we are here something
              # has gone bady wrong!
              msg <- glue::glue("Bummer - no data for {the.table} station {the.station}
                               even though there should be")
              stop(msg)
            }

            the.earliest_date <- dat$date_local[1]

            # Check that the time series is long enough
            ndays = max(dat$date_local) - min(dat$date_local)
            if (ndays > 21) {
              res <- CERMBffdi::calculate_ffdi(dat,
                                               av.rainfall = the.avrain,
                                               datatype = the.table,
                                               for.records = "all")

              res <- res %>%
                dplyr::select(station,
                              date_local = date, hour_local = hour, min_local = minute,
                              tmaxdaily, precipdaily, kbdi, drought, ffdi)

              # Set FFDI quality based on the lowest quality class of
              # input variables. Quality classes (highest to lowest) are
              #
              #  Y: quality controlled and acceptable
              #  N: not quality controlled
              #  X: one or more input variables with missing quality code
              #  I: quality controlled and inconsistent with other known information
              #  S: quality controlled and considered suspect
              #  W: quality controlled and considered wrong
              #
              q_in <- dat %>%
                dplyr::select(precipitation_quality,
                              relhumidity_quality,
                              temperature_quality,
                              windspeed_quality) %>%

                dplyr::mutate(dplyr::across(dplyr::everything(), quality_na2x)) %>%

                as.matrix()


              res$ffdi_quality <- apply(q_in, 1, function(qrow) {
                intersect(c("W", "S", "I", "X", "N", "Y"), qrow)[1]
              })

              ii <- is.na(res$ffdi)
              res$ffdi_quality[ii] <- NA


              # Unless recalculate is TRUE, only transfer
              # the newly calculated records. Just to be safe, don't
              # assume that `dat` and `res` have the same number and order
              # of records, even though they should.
              #
              if (!recalculate) {
                res <- dat %>%
                  dplyr::transmute(date_local, hour_local, min_local, had_ffdi = !is.na(ffdi)) %>%

                  dplyr::left_join(res, by = c("date_local", "hour_local", "min_local")) %>%

                  # Retain records that have FFDI in `res` but did not in `dat`
                  dplyr::filter(!is.na(ffdi), !had_ffdi) %>%

                  dplyr::select(-had_ffdi)
              }

              if (nrow(res) > 0) {
                temptbl.name <- paste0(the.table, "_ffdi_temp")

                sql.create_temp_table <- glue::glue("
                CREATE TEMPORARY TABLE {temptbl.name}
                ON COMMIT DROP
                AS
                SELECT station, date_local, hour_local, min_local,
                tmaxdaily, precipdaily, kbdi, drought, ffdi, ffdi_quality
                FROM {the.table}
                WITH NO DATA;")

                sql.index_temp_table <- glue::glue("
                CREATE INDEX idx_tmp_date_local
                ON {temptbl.name} USING btree
                (date_local ASC);")

                sql.analyze_temp_table <- glue::glue("
                ANALYZE {temptbl.name};")

                sql.update_recs <- glue::glue("
                UPDATE {the.table} m
                SET tmaxdaily = t.tmaxdaily,
                precipdaily = t.precipdaily,
                kbdi = t.kbdi,
                drought = t.drought,
                ffdi = t.ffdi,
                ffdi_quality = t.ffdi_quality
                FROM {temptbl.name} t
                WHERE m.station = t.station
                AND m.date_local = t.date_local
                AND m.hour_local = t.hour_local
                AND m.min_local = t.min_local;")

                sql.drop_temp_table <- glue::glue("
                DROP TABLE {temptbl.name};")

                if (.is_pool_connection(db)) {
                  pool::poolWithTransaction(db, function(conn) {
                    DBI::dbExecute(conn, sql.create_temp_table)
                    DBI::dbWriteTable(conn, temptbl.name, res, overwrite = TRUE)

                    DBI::dbExecute(conn, sql.index_temp_table)
                    DBI::dbExecute(conn, sql.analyze_temp_table)

                    DBI::dbExecute(conn, sql.update_recs)

                    DBI::dbExecute(conn, sql.drop_temp_table)
                  })
                } else if (.is_dbi_connection(db)) {
                  DBI::dbWithTransaction(db, {
                    DBI::dbExecute(db, sql.create_temp_table)
                    DBI::dbWriteTable(db, temptbl.name, res, overwrite = TRUE)

                    DBI::dbExecute(conn, sql.index_temp_table)
                    DBI::dbExecute(conn, sql.analyze_temp_table)

                    DBI::dbExecute(conn, sql.update_recs)

                    DBI::dbExecute(conn, sql.drop_temp_table)
                  })
                }

              } # end of writing records to db

            } # end of check for ndays > 21

          } # end of check for FFDI calculation required

        } # end of station loop

      } # end of check for 1 or more selected stations

    } # end of state loop


    # Refresh the materialized view for FFDI dates
    message(glue::glue("Refreshing materialized view {the.table}_ffdi_dates"))

    cmd <- glue::glue("REFRESH MATERIALIZED VIEW CONCURRENTLY {the.table}_ffdi_dates;")
    DBI::dbExecute(db, cmd)
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

