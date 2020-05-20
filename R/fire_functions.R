#' Calculate values of fire-related variables in the database tables
#'
#' This function calculates and stores values for the database fields 'kbdi'
#' (Keetch-Bryam Drought Index), 'drought' (Drought Factor), and 'ffdi' (Forest
#' Fire Danger Index) for records in one or both of the 'AWS' and 'Synoptic'
#' database tables. Calculations are performed by the function
#' \code{bom_db_calculate_ffdi}. Depending on the value of the \code{records}
#' argument, values can be calculated for all records (any previous values are
#' replaced), or only for those records with more recent dates and times than
#' the last record with non-missing values. The records must cover a continuous
#' time series, i.e. no gaps in the sequence of dates.
#'
#' KBDI calculations require an average annual precipitation value. This can
#' either be taken from the station values in the \code{\link{STATION_METADATA}}
#' table, derived from the NARCLIM 'p12' layer, or calculated from stations
#' records for the 2001-2015 period. The default, and recommended option, is to
#' use the NARCLIM station values. Note that at present the database does not
#' record which option was used so it is up to you to be consistent (!)
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param tables Names of tables to update. The default is to only update the
#'   'AWS' table. Table names may be abbreviated.
#'
#' @param records The records to update: either \code{'new'} (default) to only
#'   update records with more recent dates and times than the last record with
#'   calculated values; or \code{'all'} to calculate values for all records in a
#'   table, replacing any previously calculated values. Note that for both
#'   options, values can only be calculated for records that form a continuous
#'   time series.
#'
#' @param stations An optional vector of integer station numbers. If provided,
#'   updates will only be performed for these stations. The default is to
#'   update all stations.
#'
#' @param av.rainfall.method Either 'metadata' (default and recommended) to take
#'   average rainfall values from the \code{STATIONS_METADATA} table; or
#'   'records' to calculate average rainfall from station records over the period
#'   2001 - 2015. With the latter option, any station that does not have data
#'   for the whole of the reference period will be ignored.
#'
#' @param min.days.required The minimum number of days of data required to
#'   perform calculations for a station. The default and smallest allowable
#'   value is 30 days. The number of days is determined by counting distict
#'   record dates. This does not consider gaps (missing days) in the time
#'   series.
#'
#' @param quiet If \code{TRUE}, do not print progress information (table and
#'   station names) to the console during processing. Default is \code{FALSE}.
#'
#' @return A vector with named elements giving the number of records updated by
#'   table.
#'
#' @seealso \code{\link{bom_db_calculate_ffdi}}
#'
#' @export
#'
bom_db_update_fire <- function(db,
                               tables = c("AWS", "Synoptic"),
                               records = c("new", "all"),
                               stations = NULL,
                               av.rainfall.method = c("metadata", "records"),
                               min.days.required = 30,
                               quiet = FALSE) {
  .ensure_connection(db)

  records <- match.arg(records)

  av.rainfall.method <- match.arg(av.rainfall.method)

  if (min.days.required < 30) {
    message("min.days.required reset to minimum valid value: 30")
    min.days.required <- 30
  }

  # Case-insensitive match for table names
  tables <- unique(match.arg(tolower(tables),
                             c("aws", "synoptic"),
                             several.ok = TRUE))

  res <- lapply(tables, function(the.table) {
    table.stations <- stations
    if (length(table.stations) == 0) {
      cmd <- glue::glue("select distinct station
                        from {the.table} order by station;")

      table.stations <- pool::dbGetQuery(db, cmd) %>%
        dplyr::pull(station)
    }

    Nstns <- length(table.stations)
    nrecs.updated <- sapply(1:Nstns, function(istn) {
      the.station <- table.stations[istn]
      if (!quiet) {
        msg <- glue::glue("{the.table}: {the.station} ({istn}/{Nstns})")
        message(msg)
      }
      .do_update_fire(db, the.table, the.station, records, av.rainfall.method)
    })

    data.frame(table = the.table,
               station = table.stations,
               nrecs = nrecs.updated,
               stringsAsFactors = FALSE)
  })

  res <- do.call(rbind, res)

  res
}


.do_update_fire <- function(db,
                            the.table,
                            the.station,
                            records,
                            av.rainfall.method,
                            min.days.required) {

  stopifnot(length(the.table) == 1,
            the.table %in% c("aws", "synoptic"))

  stopifnot(length(the.station) == 1)

  stopifnot(length(records) == 1,
            records %in% c("new", "all"))

  stopifnot(length(av.rainfall.method) == 1,
            av.rainfall.method %in% c("metadata", "records"))

  # Constants used if av.rainfall.method is 'records'
  AvRainfallYears <- 2001:2015
  MinDaysPerYear <- 300

  nrecs.updated <- 0

  # Check number of days for station to see if it's worth doing
  # FFDI calculations
  cmd <- glue::glue("select count(*) as ndays from
                       (select distinct year, month, day
                        from {the.table}
                        where station = {the.station});")

  res <- pool::dbGetQuery(db, cmd)
  if (res$ndays < 30) {
    msg <- glue::glue("Skipping station {the.station}: \\
                       only has data for {res$ndays} days")
    warning(msg, immediate. = TRUE)
    return(0)
  }

  cmd <- glue::glue("select * from {the.table}
                    where station = {the.station}
                    order by year, month, day, hour, minute;")

  dat <- pool::dbGetQuery(db, cmd)
  if (nrow(dat) == 0) return (0)

  dat$precipitation <- .na2zero(dat$precipitation)

  if (av.rainfall.method == "metadata") {
    i <- which(CERMBweather::STATION_METADATA$station == the.station)
    if (length(i)) {
      av.rainfall <- CERMBweather::STATION_METADATA$annualprecip_narclim[i]
      if (is.na(av.rainfall)) {
        msg <- glue::glue("Skipping station {the.station}: \\
                           missing rainfall value in STATION_METADATA")
        warning(msg, immediate. = TRUE)
        return(0)
      }
    } else {
      msg <- glue::glue("Skipping station {the.station}: \\
                         not found in STATION_METADATA")
      warning(msg, immediate. = TRUE)
      return(0)
    }

  } else if (av.rainfall.method == "records") {
    # Check if average rainfall can be determined for the reference period
    xdat <- dat %>%
      dplyr::filter(year %in% AvRainfallYears)

    xcheck <- xdat %>%
      dplyr::group_by(year) %>%
      dplyr::summarize(ndays = dplyr::n_distinct(month, day))

    if (!all(AvRainfallYears %in% xcheck$year) ||
        !all(xcheck$ndays >= MinDaysPerYear)) {
      msg <- glue::glue("Skipping station {the.station}: \\
                         cannot calculate average rainfall")
      message(msg)
      return(0)
    }

    xdat <- dat %>%
      dplyr::filter(year %in% AvRainfallYears) %>%
      bom_db_daily_rainfall(datatype = the.table, crop = TRUE, on.error = "null")

    if (is.null(xdat)) {
      msg <- glue::glue("Skipping station {the.station}: interrupted time series")
      warning(msg)
      return(0)
    }

    xdat <- xdat %>%
      dplyr::group_by(year) %>%
      dplyr::summarize(totalrain = sum(precipdaily))

    av.rainfall <- mean(xdat$totalrain)
  }

  if (records == "new") {
    # Subset to records at the end of the time series that do
    # not have FFDI values plus 20 days of preceding records
    # (for drought factor calculation)
    irec <- .find_na_tail(dat$ffdi)

    if (is.na(irec)) {
      # Last record has FFDI value - nothing to do
      return(0)

    } else {
      # Date of earliest record in subset
      date.missing <- .ymd_to_date(dat$year[irec], dat$month[irec], dat$day[irec])

      # Need 20 days of previous data for drought factor calculation
      # (or all previous records if less than 20 days exist)
      date.start <- date.missing - 20

      dat <- dat %>%
        dplyr::filter(.ymd_to_date(year, month, day) >= date.start)

      # Check whether the data records form an uninterrupted daily
      # time series
      check <- bom_db_check_datetimes(dat, daily = FALSE)[[1]]
      if (!check$ok) {
        # if there are gaps we can only work with the most recent
        # uninterrupted subset
        gap.date <- tail(check$gaps, 1)
        ymd.start <- .date_to_ymd(gap.date)

        dat <- dat %>%
          dplyr::filter(year >= ymd.start$year,
                        month >= ymd.start$month,
                        day >= ymd.start$day)
      }
    }
  } else {  # records == "all"
    # Reset fire var values
    dat <- dplyr::mutate(dat,
                         kbdi = NA_real_,
                         drought = NA_real_,
                         ffdi = NA_real_)
  }

  # Allow for calculation failing in the case of an interrupted time
  # series not detcted earlier
  dat.ffdi <- NULL
  tryCatch({
    dat.ffdi <- bom_db_calculate_ffdi(dat,
                                      av.rainfall = av.rainfall,
                                      datatype = the.table)
  }, error = function(e) {
    warning(e$message, "\n", immediate. = TRUE)
  })

  if (is.null(dat.ffdi)) return(0)

  # Update the source table
  #
  dat.repl <- dat %>%
    dplyr::select(-(tmaxdaily:ffdi)) %>%
    dplyr::left_join(dat.ffdi, by = c("station", "year", "month", "day", "hour", "minute"))

  nrecs.updated <- pool::poolWithTransaction(DB, function(con) {
    DBI::dbExecute(DB, "drop table if exists ffdi_tmp;")

    DBI::dbWriteTable(con, "ffdi_tmp", dat.repl)

    varnames <- paste(colnames(dat.repl), collapse = ", ")
    placeholders <- paste(paste0(":", colnames(dat.repl)), collapse = ", ")

    cmd <- glue::glue("replace into {the.table}
                    ({varnames})
                    VALUES ({placeholders});")

    res <- DBI::dbSendStatement(con, cmd)
    res <- DBI::dbBind(res, params = dat.repl)
    nrecs <- DBI::dbGetRowsAffected(res)
    DBI::dbClearResult(res)

    DBI::dbExecute(con, "drop table ffdi_tmp;")

    nrecs
  })

  nrecs.updated
}


#' Calculate FFDI for a given data set.
#'
#' This function calculates FFDI (Forest Fire Danger Index) and the related
#' variables: KBDI (Keetch-Bryam Drought Index) and Drought Factor. You can call
#' it directly to calculate FFDI for an arbitrary data set of weather data for
#' one or more stations (see Arguments below for the required format). The
#' function is also used by \code{bom_db_update_fire} which sets the values of
#' fire-related variables in the database tables.
#'
#' If there are missing days in the time series, dummy records will be added
#' with all weather variables set to \code{NA}. This allows KBDI, drought factor
#' and FFDI to be calculated for the rest of the time series. FFDI values for
#' the dummy records will be set to \code{NA}. KBDI values (and resulting
#' drought factor values) are set following the protocol described for
#' \code{\link{bom_db_kbdi}}. The records for each station must cover a period
#' of at least 21 days for FFDI to be calculated, since Drought Factor requires
#' an initial 20 days of daily rainfall data.
#'
#' @param dat A data frame with records for one or more weather stations.
#'   Normally this will made up of records returned from querying the synoptic
#'   or AWS tables in the database. If from another data source, the columns
#'   and column names must conform to those used in the database tables. The
#'   required columns are:
#'   \code{'station', 'year', 'month', 'day', 'hour', 'minute',
#'         'precipitation', 'temperature', 'relhumidity'}
#'   plus the specified wind speed column (default: \code{'windspeed'}).
#'
#' @param av.rainfall Average annual rainfall value to use in the calculation of
#'   KBDI (on which FFDI relies). Note: if the data include multiple weather
#'   stations, the same average rainfall value is applied to all.
#'
#' @param datatype Either a specified data type ('AWS' or 'Synoptic') or 'guess'
#'   (default) to guess the type from the data. Case-insensitive and may be
#'   abbreviated. The data type controls how rainfall values are aggregated to
#'   daily values for the calculation of KBDI and drought factor. AWS rainfall
#'   values are total since 9am, whereas Synoptic values are rainfall for the
#'   record time step. If 'guess', the function checks whether non-zero rainfall
#'   values are always ascending during each 24 hour period to 9am which
#'   indicates AWS data.
#'
#' @param windcol Name of the column of wind speed values to use for FFDI
#'   calculation. The default is to use \code{'windspeed'}. AWS data also has
#'   a \code{'windgust'} column (maximum wind gust in previous 10 minutes).
#'
#' @return A data frame with columns:
#'   \code{'station', 'year', 'month', 'day', 'hour', 'minute',}
#'   \code{'tmaxdaily'} (maximum temperature for calendar day),
#'   \code{'precipdaily'} (total rainfall from 09:01 to 09:00 the next day),
#'   \code{'kbdi'} (Keetch-Bryam drought index),
#'   \code{'drought'} (daily drought factor values),
#'   \code{'ffdi'} (FFDI value based on daily drought factor and time-step
#'     values for temperature, wind speed and relative humidity.
#'
#' @examples
#' \dontrun{
#' library(CERMBweather)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' dat.ffdi <- bom_db_calculate_ffdi(dat.stations, av.rainfall = 800)
#'
#' # If you want to relate FFDI and drought factor values to the
#' # original sub-daily weather values, join to input data
#' dat.ffdi <- left_join(dat.stations, dat.ffdi,
#'                   by = c("station", "year", "month", "day", "hour", "minute"))
#' }
#'
#' @seealso \code{\link{bom_db_update_fire}}
#'
#' @export
#'
bom_db_calculate_ffdi <- function(dat,
                                  av.rainfall = NULL,
                                  datatype = c("guess", "AWS", "Synoptic"),
                                  windcol = "windspeed") {

  colnames(dat) <- tolower(colnames(dat))

  RequiredCols <- c("station", "year", "month", "day", "hour", "minute",
                    "precipitation", "temperature", "relhumidity")

  .require_columns(dat, RequiredCols)

  datatype <- match.arg(tolower(datatype), c("guess", "aws", "synoptic"))
  if (datatype == "guess") {
    datatype <- .guess_data_type(dat)
  }

  av.rainfall <- av.rainfall[1]
  if (is.null(av.rainfall) || av.rainfall <= 0) {
    stop("A valid value for average rainfall is required")
  }

  windcol <- tolower(windcol)
  if (!(windcol %in% colnames(dat))) {
    stop("Missing wind speed column: ", windcol)
  }

  stations <- unique(dat$station)
  nstns <- length(stations)

  res <- lapply(stations, function(the.stn) {
    dat.stn <- dat %>%
      dplyr::filter(station == the.stn)

    .do_calculate_ffdi(dat.stn,
                       datatype,
                       av.rainfall,
                       windcol)
  })

  dplyr::bind_rows(res)
}


# Private worker function for bom_db_ffdi
#
.do_calculate_ffdi <- function(dat.stn,
                               datatype,
                               av.rainfall = NULL,
                               windcol = NULL) {

  # Sanity checks on the upstream call
  stopifnot(dplyr::n_distinct(dat.stn$station) == 1)

  stopifnot(datatype %in% c("aws", "synoptic"))

  stopifnot(!is.null(av.rainfall))
  stopifnot(length(av.rainfall) == 1 && av.rainfall > 0)

  stopifnot(!is.null(windcol))
  stopifnot(windcol %in% colnames(dat.stn))

  # Rename the wind speed column to be used for FFDI, giving it a
  # standard name. This makes the calculation code at the end of
  # this function a bit easier.
  dat.stn <- dplyr::rename_at(dat.stn, windcol, ~"..ffdi_wind..")

  # Check the time series is long enough for KBDI and drought
  # calculations which need 20 prior days of rainfall data
  dat.stn <- dat.stn %>%
    dplyr::arrange(year, month, day, hour, minute)

  d0 <- .ymd_to_date(dat.stn[1, c("year", "month", "day")])
  d1 <- .ymd_to_date(dat.stn[nrow(dat.stn), c("year", "month", "day")])
  ndays <- d1 - d0
  if (ndays < 21) {
    the.station <- dat.stn$station[1]
    msg <- glue::glue(
      "Records for station {the.station} span {ndays} days but \\
       at least 21 days are required to calculate FFDI")
    warning(msg, immediate. = TRUE)
    return(NULL)
  }

  dat.stn <- dat.stn %>%
    # Ensure only one record per time point (this will arbitrarily
    # take the first from any duplicate set)
    dplyr::distinct(year, month, day, hour, minute, .keep_all = TRUE) %>%

    # Treat missing rainfall values as zero (least worst option)
    dplyr::mutate(precipitation = .na2zero(precipitation))


  # Add dummy records for any missing days. Dates of dummy records
  # are returned as an attribute.
  dat.stn <- .add_missing_days(dat.stn)
  dates.added <- attr(dat.stn, "dates.added", exact = TRUE)
  attr(dat.stn, "dates.added") <- NULL

  # Tmax by calendar day
  #
  dat.daily <- dat.stn %>%
    dplyr::group_by(year, month, day) %>%
    dplyr::summarize(tmaxdaily = .max_with_na(temperature)) %>%
    dplyr::ungroup()

  # Add daily rainfall data
  dat.daily <- dat.daily %>%
    dplyr::left_join(
      bom_db_daily_rainfall(dat.stn, datatype = datatype),
      by = c("year", "month", "day")
    )

  # Guard against a missing daily rainfall value which can arise
  # when the last rain day's data did not include the full
  # 09:01-09:00 period
  dat.daily$precipdaily <- .na2zero(dat.daily$precipdaily)

  # Ensure record order
  dat.stn <- dat.stn %>%
    dplyr::ungroup() %>%
    dplyr::arrange(year, month, day)


  # If previously calculated KBDI and drought values have been provided,
  # join these to the daily data (only join drought if KBDI is also present)
  if ("kbdi" %in% colnames(dat.stn)) {
    tmp <- dat.stn

    if ( !("drought" %in% colnames(dat.stn)) ) {
      tmp$drought <- NA
    }

    tmp <- tmp %>%
      dplyr::group_by(year, month, day) %>%
      dplyr::summarize(kbdi = dplyr::first(kbdi),
                       drought = dplyr::first(drought))

    dat.daily <- dat.daily %>%
      dplyr::left_join(tmp, by = c("year", "month", "day"))
  }

  dat.daily <- dat.daily %>%
    bom_db_kbdi(average.rainfall = av.rainfall, assume.order = TRUE) %>%
    bom_db_drought(assume.order = TRUE)


  # Drop any pre-existing columns for daily data from the station data
  # before joining the newly calculated daily data
  cols <- setdiff(colnames(dat.daily), c("year", "month", "day"))
  to.drop <- colnames(dat.stn) %in% cols
  dat.stn <- dat.stn[, !to.drop]

  # Join daily values to original time-step data
  dat.stn <- dat.stn %>%
    dplyr::left_join(dat.daily, by = c("year", "month", "day")) %>%

    # Add calculated FFDI values.
    # Missing values in any of the input variables will propagate
    # through to FFDI.
    dplyr::mutate(ffdi = round(
      2 * exp(0.987 * log(drought) - 0.45 +
                0.0338 * temperature +
                0.0234 * ..ffdi_wind.. -  # using standard colname set earlier
                0.0345 * relhumidity),
      digits = 1))

  # Remove any dummy records that were added for missing days
  if (length(dates.added) > 0) {
    ymd <- .date_to_ymd(dates.added)
    dat.stn <- dat.stn %>%
      dplyr::anti_join(ymd, by = c("year", "month", "day"))
  }

  dat.stn %>%
    dplyr::select(station, year, month, day, hour, minute,
                  tmaxdaily, precipdaily, kbdi, drought, ffdi)
}


#' Calculate daily KBDI values from data for a single weather station
#'
#' Calculates daily values for the Keetch-Byram drought index. Following Finkele
#' et al. (2006), KBDI is calculated as yesterday's KBDI plus
#' evapo-transpiration minus effective rainfall. Input is a data frame of daily
#' weather data. This must have columns: year, month, day, precipdaily,
#' tmaxdaily. Optionally, it can also have a column: kbdi. If present,
#' calculations will be done for the most recent block of records with missing
#' values, with preceding records being used to initialize the KBDI time series.
#' If a kbdi column is absent, or present but with all missing values,
#' calculations will be performed for all records using the default
#' initialization in which KBDI is set to its maximum value (203.2).
#'
#' Dealing with missing values: Nny missing daily precipitation values are set
#' to zero and a message is issued, Missing daily temperature values are left
#' as-is. The value of KBDI depends on the previous day's value. A missing value
#' in the daily time series for temperature leads to a missing value for KBDI,
#' which would then propagate to the next and all subsequent days. To avoid
#' this, if the KBDI value for the previous day is missing, the function uses
#' the most recent non-missing value from the previous 30 days. If no
#' non-missing value is available, the maximum value of 203.2 is used.
#'
#' @param dat.daily A data frame of daily weather data. This should be for a
#'   single weather station. If a 'station' column is present this will be
#'   checked, otherwise it is assumed. Must have columns: year, month, day,
#'   precipdaily, tmaxdaily. Optionally, it can also have column: kbdi.
#'   Additional columns are permitted but will be ignored. See Description for
#'   details.
#'
#' @param average.rainfall Reference value for average annual rainfall.
#'
#' @param assume.order If \code{TRUE} the daily data are assumed to be in
#'   calendar order and form an uninterrupted time series. If \code{FALSE}
#'   (default) this will be checked. This option provides for more efficient
#'   processing as part of a pipeline, but should be left at the default when
#'   the function is called directly.
#'
#' @return A data frame with columns: year, month, day, kbdi.
#'
#' @export
#'
bom_db_kbdi <- function(dat.daily,
                        average.rainfall,
                        assume.order = FALSE) {

  colnames(dat.daily) <- tolower(colnames(dat.daily))

  # If there is a station column, check there is only a single station
  if (("station" %in% colnames(dat.daily)) &&
      dplyr::n_distinct(dat.daily$station) > 1) {
    stop("This function presently only works with data for a single station")
  }

  RequiredCols <- c("year", "month", "day", "precipdaily", "tmaxdaily")
  .require_columns(dat.daily, RequiredCols)

  if (!assume.order) {
    check <- bom_db_check_datetimes(dat.daily, daily = TRUE)[[1]]
    if (!check$ok) stop(check$err)
  }

  if (anyNA(dat.daily$precipdaily)) {
    message("bom_db_kbdi: setting missing precipitation values to zero")
    dat.daily$precipdaily <- .na2zero(dat.daily$precipdaily)
  }

  InputKBDI <- "kbdi" %in% colnames(dat.daily)

  if (InputKBDI) {
    # Identify most recent period with no KBDI values
    start.rec <- .find_na_tail(dat.daily$kbdi)
    if (is.na(start.rec)) {
      # Last rec has KBDI value - nothing to do
      message("Final day has KBDI value already. Nothing to do.")
      return(dat.daily)
    }
  } else {
    start.rec <- 1
  }

  Ndays <- nrow(dat.daily)
  ET <- numeric(Ndays)

  if (InputKBDI) {
    Kday <- dat.daily$kbdi
  } else {
    Kday <- numeric(Ndays)
  }

  precipdaily.eff <- bom_db_effective_rainfall(dat.daily$precipdaily)

  # Term 3 in ET equation, annual rainfall influence
  ET3 <- 1 + 10.88 * exp(-.001736 * average.rainfall)

  # Term 4 in ET equation
  ET4 <- 1e-3

  # First step requires at least two days of KBDI values.
  if (start.rec == 1) {
    # Starting from scratch - set the first two days to maximum
    # dryness value
    Kday[1:2] <- 203.2
  } else if (start.rec == 2) {
    #
    Kday[2] <- Kday[1]
  }

  # start.rec should be >= 3 for loop below
  start.rec <- max(3, start.rec)

  # Helper for KBDI loop below
  clampK <- function(x) {
    ifelse(is.na(x), NA, min(203.2, max(0, round(x, digits = 1))))
  }

  for (i in start.rec:Ndays) {
    Kprev <- Kday[i-1]

    if (is.na(Kprev)) {
      # [Hamish - comment from MATLAB code]
      # Missing Tmax or precip.eff values result in all subsequent
      # ET and KBDI values being set to missing because of the lagged effect
      # of these variables. We need to specify when it is OK to ignore previous
      # missing values.
      #
      # In the approach used here, if Tmax and rain values are present
      # for days i and i-1, then we use the most recent non-missing
      # KBDI value from not more than 30 days previously. If no previous
      # data are available for that period, we set previous KBDI to
      # maximum deficit of 203.2
      #
      Krecent <- na.omit( Kday[(i-2):max(1, i-30)] )
      if (length(Krecent) > 0) {
        # most recent non-missing value
        Kprev <- Krecent[1]
      } else {
        # no values available - set to max deficit
        Kprev <- 203.2
      }
    }

    ET1 <- 203.2 - Kprev
    ET2 <- 0.968 * exp(0.0875 * dat.daily$tmaxdaily[i-1] + 1.5552) - 8.30
    ET[i] <- ((ET1 * ET2) / ET3) * ET4

    Kday[i] <- clampK( Kprev - precipdaily.eff[i] + ET[i] )
  }

  dat.daily$kbdi <- Kday

  dat.daily
}


#' Calculate daily drought factor values from data for a single weather station
#'
#' Calculates daily values for drought factor.Input is a data frame of daily
#' weather and KBDI data. This must have columns: year, month, day, precipdaily,
#' tmaxdaily, kbdi. Optionally, it can also have a column: drought. If present,
#' calculations will be done for the most recent block of records with missing
#' values, with preceding records being used to initialize the drought factor
#' time series. If a drought column is absent, or present but with all missing
#' values, calculations will be performed for all records using the default
#' initialization in which KBDI is set to its maximum value (203.2).
#'
#' Drought factor is based on a moving window of the previous 20 days. Within
#' this window we search for a rainfall event, defined as consecutive days, each
#' with rain >2mm. If an event occurs we determine the total rainfall over the
#' event, and the number of days since the day of highest rainfall within the
#' event. The resulting values are then combined with KBDI in some darkly
#' mysterious way.
#'
#' @param dat.daily A data frame of daily weather data. This should be for a
#'   single weather station. If a 'station' column is present this will be
#'   checked, otherwise it is assumed. Must have columns: year, month, day,
#'   precipdaily, kbdi. Optionally, it can also have column: drought. Additional
#'   columns are permitted but will be ignored. See Description for details.
#'
#' @param assume.order If \code{TRUE} the daily data are assumed to be in
#'   calendar order and form an uninterrupted time series. If \code{FALSE}
#'   (default) this will be checked. This option provides for more efficient
#'   processing as part of a pipeline, but should be left at the default when
#'   the function is called directly.
#'
#' @return A data frame with columns: year, month, day, kbdi, drought.
#'
#' @export
#'
bom_db_drought <- function(dat.daily, assume.order = FALSE) {

  colnames(dat.daily) <- tolower(colnames(dat.daily))

  # If there is a station column, check there is only a single station
  if (("station" %in% colnames(dat.daily)) &&
      dplyr::n_distinct(dat.daily$station) > 1) {
    stop("This function presently only works with data for a single station")
  }

  RequiredCols <- c("year", "month", "day", "precipdaily", "kbdi")
  .require_columns(dat.daily, RequiredCols)

  if (!assume.order) {
    check <- bom_db_check_datetimes(dat.daily, daily = TRUE)[[1]]
    if (!check$ok) stop(check$err)
  }

  Ndays <- nrow(dat.daily)

  InputDF <- "drought" %in% colnames(dat.daily)

  # Check there are enough days to do something
  if (Ndays < 21) {
    message("Too few days (min is 21) to calculate drought factor")

    # Add a drought column if there is not already one and return
    if (!InputDF) dat.daily$drought <- NA_real_
    return(dat.daily)
  }

  if (InputDF) {
    DFday <- dat.daily$drought

    # Identify most recent period with no drought values
    start.rec <- .find_na_tail(dat.daily$drought)
    if (is.na(start.rec)) {
      # Last rec has drought value - nothing to do
      message("Final day has drought factor value already. Nothing to do.")
      return(dat.daily)
    }
  } else {
    DFday <- rep(NA_real_, Ndays)
    start.rec <- 21
  }

  # start.rec should be >= 21 for loop below
  start.rec <- max(21, start.rec)

  # Only rainfall of 2mm or more is considered
  prDF <- ifelse(dat.daily$precipdaily > 2, dat.daily$precipdaily, 0)

  for (i in start.rec:Ndays) {
    if (is.na(dat.daily$kbdi[i]) || is.na(dat.daily$precipdaily[i])) {
      DFday[i] <- NA

    } else {
      # Find most recent rainfall event not more than 20 days ago
      ii <- (i-1):(i-20)

      r20 <- prDF[ii]
      r20.flag <- prDF[ii] > 0

      # Look for instances of two consecutive days with rain
      is.event <- r20.flag & dplyr::lead(r20.flag)

      # last is.event element will be NA, change to FALSE
      is.event[length(r20.flag)] <- FALSE

      if (!any(is.event)) {
        # No runs of two or more rainy days
        xraw <- 1

      } else {
        # start index of most recent event
        ev.start <- which(is.event)[1]

        # end index of most recent event
        ii <- which(!r20.flag)
        ii <- ii[ii > ev.start]
        ev.end <- ifelse(length(ii) == 0, length(r20.flag), ii[1] - 1)

        # event rainfall
        E <- r20[ev.start:ev.end]
        P <- sum(E)

        # index of maximum rainfall in the window
        # (days since most rain)
        Emax.i <- which.max(E) + ev.start - 1

        # Following Finkele et al. (2006)
        Emax.term <- ifelse(Emax.i == 1, 0.8, Emax.i - 1)
        xraw <- (Emax.term^1.3) / (Emax.term^1.3 + P - 2)
      }

      if (dat.daily$kbdi[i] >= 20) {
        xlim <- 75 / (270.525 - 1.267 * dat.daily$kbdi[i])
      } else {
        xlim <- 1 / (1 + 0.1135 * dat.daily$kbdi[i])
      }

      x <- min(xraw, xlim)

      xfactor <- (41 * x^2 + x) / (40 * x^2 + x + 1)

      DFbase <- 10.5* (1.0 - exp(-(dat.daily$kbdi[i] + 30) / 40))
      DFraw <- DFbase * xfactor

      # If DFraw is NA, allow this to propagate to DFday
      DFday[i] <- round( min(10, DFraw), digits = 1)
    }
  }

  dat.daily$drought <- DFday

  dat.daily
}


#' Calculate effective rainfall values from daily values
#'
#' For KBDI calculation (Finkele et al. 2006), the first 5mm of rain
#' do not count towards evapo-transpiration. If it rains less than 5mm over
#' several days, a running balance is kept. Once this balance exceeds 5mm, the
#' first 5mm is removed for canopy interception/runoff and the remainder
#' constitutes effective rainfall. Any day without rain resets the balance to
#' zero.
#'
#' @param precipdaily Vector of daily precipitation values.
#'
#' @param start.balance Starting balance. Should be a value between 0 and 5
#'   (default is 0). A value outside this range will be silently set to 0 or 5.
#'
#' @return A vector of effective rainfall values.
#'
#' @export
#'
bom_db_effective_rainfall <- function(precipdaily, start.balance = 0) {
  if (anyNA(precipdaily)) stop("Missing values in input vector for daily precipitation")

  start.balance <- min(5, max(0, start.balance[1]))

  Ndays <- length(precipdaily)
  peff <- numeric(Ndays)
  bal <- start.balance

  for (i in 1:Ndays) {
    bal.prev <- bal

    if (precipdaily[i] == 0) {
      # reset for a day with no rain
      bal <- 0
    } else {
      if (bal.prev == 0) {
        # some rain this day and
        # none previously
        if (precipdaily[i] > 5) {
          # remove interception/runoff then set as Peff
          peff[i] <- precipdaily[i] - 5
          # Set bal to 5 so any rain in next day is added
          # without removing 5
          bal <- 5
        } else {
          # no effective rain but the balance increases
          bal <- precipdaily[i]
        }
      } else if (bal.prev < 5) {
        # some rain this day and
        # less than five previously
        if (precipdaily[i] + bal.prev > 5) {
          # remove interception/runoff, then set as Peff
          peff[i] <- precipdaily[i] + bal.prev - 5
          # Set bal to 5 so any rain in next day is added
          # without removing 5
          bal <- 5
        } else {
          # still haven't exceeded 5mm
          bal <- precipdaily[i] + bal.prev
        }
      } else {
        # interception/runoff has already been removed
        # so add all rain to Peff keep balance at 5
        peff[i] <- precipdaily[i]
        bal <- 5
      }
    }
  }

  peff
}


#' Aggregate rainfall values to daily time steps
#'
#' This is primarily a helper function called by other functions when
#' calculating fire-related variables. It aggregates sub-daily rainfall data to
#' daily values, taking into account the different conventions used for AWS and
#' Syoptic data sources. Each value for each day is the total rainfall recorded
#' from 09:01 that day to 09:00 the following day. If there are any missing days
#' in the time series, the function will either stop with an error or return
#' \code{NULL} depending on the value of the \code{on.error} argument.
#'
#' @param dat A data set of sub-daily weather records for either an AWS or a
#'   Synoptic source.
#'
#' @param datatype Source type of data. Default is to guess based on data
#'   values.
#'
#' @param crop If \code{TRUE} (default), discard the rainfall for an initial
#'   part day.
#'
#' @param on.error Action to perform if there is any interruption in the time
#'   series. If \code{'stop'}, the function will stop with an error message. If
#'   \code{'null'}, the function will return NULL.
#'
#' @return A data frame with columns: station (if present in the input data),
#'   year, month, day, precipdaily.
#'
#' @export
#'
bom_db_daily_rainfall <- function(dat,
                                  datatype = c("guess", "aws", "synoptic"),
                                  crop = FALSE,
                                  on.error = c("stop", "null")) {

  on.error <- match.arg(on.error)

  colnames(dat) <- tolower(colnames(dat))

  RequiredCols <- c("year", "month", "day", "hour", "minute", "precipitation")
  .require_columns(dat, RequiredCols)

  HasStation <- "station" %in% colnames(dat)
  if (!HasStation) dat$station <- 0

  datatype <- match.arg(tolower(datatype), choices = c("guess", "aws", "synoptic"))
  if (datatype == "guess") {
    # check first station
    the.stn <- dat$station[1]
    datatype <- .guess_data_type(dplyr::filter(dat, station == the.stn))
  }

  # Helper function to aggregate rain-day values
  fn_rainday <- function(rain) {
    rain <- na.omit(rain)
    if (length(rain) == 0) {
      0
    } else {
      ifelse(datatype == "aws", max(rain), sum(rain))
    }
  }

  stns <- unique(dat$station)

  res <- lapply(stns, function(the.stn) {
    dat.stn <- dplyr::filter(dat, station == the.stn)

    check <- bom_db_check_datetimes(dat.stn, daily = FALSE)[[1]]

    if (!check$ok) {
      if (on.error == "null") {
        return(NULL)
      } else {
        gap.msg <- ""
        if (length(check$gaps) > 0) gap.msg <- paste(check$gaps, collapse = " ")
        msg <- glue::glue("Problem with data for station {the.stn}
                        {check$err}
                        {gap.msg}")
        stop(msg)
      }
    }

    dat.stn <- dat.stn %>%
      dplyr::arrange(year, month, day, hour, minute) %>%

      dplyr::mutate(raindate = CERMBweather:::.ymd_to_date(year, month, day),
                    timestr = sprintf("%02d%02d", hour, minute))

    # Do this bit outside of dplyr to avoid it spuriously converting
    # dates to integers
    ii <- dat.stn$timestr < "0901"
    dat.stn$raindate[ii] <- dat.stn$raindate[ii] - 1

    min.raindate <- min(dat.stn$raindate)

    dat.stn <- dat.stn %>%
      dplyr::group_by(raindate) %>%
      dplyr::summarize(precipdaily = fn_rainday(precipitation))


    if (crop) dat.stn <- dplyr::filter(dat.stn, raindate > min.raindate)

    # Add year, month day back in, and return
    dat.stn %>%
      dplyr::bind_cols(CERMBweather:::.date_to_ymd(.$raindate)) %>%
      dplyr::select(year, month, day, precipdaily)
  })

  # Check for problems
  if (on.error == "null" && any(sapply(res, is.null))) {
    return(NULL)
  }

  # Combine results for stations
  res <- dplyr::bind_rows(res)

  # If there was no station column in the input, remove the temp column
  if (!HasStation) res$station <- NULL

  res
}

