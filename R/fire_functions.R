#' Calculate FFDI for specified weather stations
#'
#' @param dat.stn A data frame with records for one or more weather stations.
#'   Normally this will made up of records returned from querying the synoptic
#'   or AWS tables in the database. If from another data source, the columns
#'   and column names must conform to those used in the database tables for
#'   variables used to calculate KBDI and FFDI values:
#'   \code{'station', 'year', 'month', 'day', 'hour', 'minute',
#'         'precipitation', 'temperature', 'relhumidity', 'windspeed'}
#'
#' @param av.rainfall.years A vector of four digit year numbers for the
#'   reference period over which average annual rainfall at each weather station
#'   should be calculated. This is used in the KBDI calculation. Normally the
#'   reference period will be a sequence of contiguous years, but it does not
#'   have to be. Default is \code{2001:2015}. Ignored if a value for
#'   \code{av.rainfall.value} is provided.
#'
#' @param min.days.per.year The minimum number of days of rainfall data required
#'   for each year specified by \code{av.rainfall.years}. Default is \code{300}.
#'   Ignored if a value for \code{av.rainfall.value} is provided.
#'
#' @param av.rainfall.value A vector of average annual rainfall values to use
#'   instead of calculating averages from weather station data. The vector
#'   should either consist of a single value to use for all weather stations in
#'   the input data, or it should provide a value for each station. The default
#'   (\code{NULL}) means to calculate average annual values from the station
#'   data.
#'
#' @return A data frame with columns:
#'   \code{'station', 'year', 'month', 'day', 'hour', 'minute',}
#'   \code{'tmax.daily'} (maximum temperature for calendar day),
#'   \code{'precip.daily'} (total rainfall from 09:01 to 09:00 the next day),
#'   \code{'drought'} (daily drought factor values),
#'   \code{'ffdi'} (FFDI value based on daily drought factor and time-step
#'   values for temperature, wind speed and relative humidity.
#'
#' @examples
#' \dontrun{
#' library(CERMBweather)
#' library(dplyr, warn.conflicts = FALSE)
#'
#' # Get synoptic data for selected weather stations
#' DB <- bom_db_open("/path/to/database/BOM.db")
#'
#' stns <- c(1006, 1007, 1019, 1020)
#'
#' dat <- bom_db_synoptic(DB) %>%
#'   filter(station %in% stns) %>%
#'   collect()
#'
#' # Calculate FFDI for stations
#' ffdi <- bom_db_ffdi(dat)
#'
#' # Calculate FFDI for stations using non-default range of years
#' # for average annual rainfall
#' ffdi <- bom_db_ffdi(dat, av.rainfall.years = 1981:2010)
#'
#' # Calculate FFDI for stations using a single value for
#' # average annual rainfall for all weather stations in the
#' # input data
#' ffdi <- bom_db_ffdi(dat, av.rainfall.value = 900)
#'
#' # If you want to relate FFDI and drought factor values to the
#' # original sub-daily weather values, join to input data
#' ffdi <- left_join(dat, ffdi,
#'                   by = c("station", "year", "month", "day", "hour", "minute"))
#'
#' bom_db_close(DB)
#' }
#'
#' @export
#'
bom_db_ffdi <- function(dat,
                        av.rainfall.years = 2001:2015,
                        min.days.per.year = 300,
                        av.rainfall.value = NULL) {

  colnames(dat) <- tolower(colnames(dat))

  RequiredCols <- c("station", "year", "month", "day", "hour", "minute",
                    "precipitation", "temperature", "relhumidity", "windspeed")

  found <- RequiredCols %in% colnames(dat)
  if (any(!found)) {
    stop("The following required columns are not present:\n",
         RequiredCols[!found])
  }

  stations <- unique(dat$station)
  nstns <- length(stations)

  if (!is.null(av.rainfall.value)) {
    nval <- length(av.rainfall.value)
    if (nval != nstns) {
      if (nval == 1) {
        av.rainfall.value <- rep(av.rainfall.value, nstns)
      } else {
        stop("av.rainfall.value should be length 1 or ", nstns, " (number of stations)")
      }
    }
  }

  res <- lapply(1:nstns, function(istn) {
    the.stn <- stations[istn]
    the.av.rainfall <- av.rainfall.value[istn]

    .do_calculate_ffdi(dat %>% dplyr::filter(station == the.stn),
                       av.rainfall.years,
                       min.days.per.year,
                       the.av.rainfall)
  })

  dplyr::bind_rows(res)
}


# Private worker function for bom_db_ffdi
#
.do_calculate_ffdi <- function(dat.stn,
                               av.rainfall.years,
                               min.days.per.year,
                               av.rainfall = NULL) {

  # Check we only have data for a single station and av.rainfall
  # (if provided) is a single positive value
  stopifnot(dplyr::n_distinct(dat.stn$station) == 1,
            is.null(av.rainfall) ||
              (length(av.rainfall) == 1 && av.rainfall > 0))

  dat.stn <- dat.stn %>%
    # Ensure only one record per time point (this will arbitrarily
    # take the first from any duplicate set)
    dplyr::distinct(year, month, day, hour, minute, .keep_all = TRUE) %>%

    # Treat missing rainfall values as zero (least worst option)
    dplyr::mutate(precipitation = ifelse(is.na(precipitation), 0, precipitation))

  if (is.null(av.rainfall)) {
    # Calculate average annual rainfall over the reference years
    x <- dat.stn %>%
      dplyr::filter(year %in% av.rainfall.years) %>%
      dplyr::group_by(year) %>%
      dplyr::summarize(ndays = dplyr::n_distinct(month, day),
                       precipitation.year = sum(precipitation))

    if (all(av.rainfall.years %in% x$year) & all(x$ndays >= min.days.per.year)) {
      av.rainfall <- mean(x$precipitation.year)

    } else {
      warning("Unable to calculate average annual rainfall for station ",
              dat.stn$station[1],
              immediate. = TRUE)

      return(NULL)
    }
  }


  # Calculate daily values for rainfall, maximum temperature and KBDI
  dat.daily <- dat.stn %>%
    # Tmax by calendar day
    dplyr::group_by(year, month, day) %>%
    dplyr::mutate(tmax.daily = max(temperature, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%

    # Rainfall for 0901 to 0900 the following day. Following
    # the reference MATLAB script, we allocate values to the
    # first of each pair of calendar days.
    dplyr::mutate(timestr = paste0(hour, minute),
                  rainday = dplyr::group_indices(., year, month, day),
                  rainday = ifelse(timestr < "0901", rainday - 1, rainday)) %>%

    dplyr::group_by(rainday) %>%
    dplyr::mutate(precip.daily = sum(precipitation)) %>%

    dplyr::group_by(year, month, day) %>%

    dplyr::summarize(tmax.daily = dplyr::first(tmax.daily),
                     precip.daily = dplyr::first(precip.daily)) %>%

    dplyr::ungroup() %>%

    dplyr::bind_cols(bom_db_drought(.$precip.daily, .$tmax.daily, av.rainfall))


  dat.stn <- dat.stn %>%
    # Join daily value of KBDI to original time-step data
    dplyr::left_join(dat.daily, by = c("year", "month", "day")) %>%

    # Add calculated FFDI values and return the data frame.
    # Missing values in any of the input variables will propagate
    # through to FFDI auto-magically.
    dplyr::mutate(ffdi = round(
      2 * exp(0.987 * log(drought) - 0.45 +
                0.0338 * temperature +
                0.0234 * windspeed -
                0.0345 * relhumidity),
      digits = 1))

  dat.stn %>%
    dplyr::select(station, year, month, day, hour, minute,
                  tmax.daily, precip.daily, kbdi, drought, ffdi)
}


#' Calculate daily drought factor
#'
#' Calculates daily drought factor based on the Keetch-Byram drought index
#' Following Finkele et al. (2006), KBDI is calculated as yesterday's KBDI
#' plus evapo-transpiration minus effective rainfall.
#'
#' @param daily.precip A vector of daily (assumed sequential) precipitation
#'   values.
#'
#' @param daily.tmax A vector of daily maximum temperature values of the same
#'   length (and assumed to be the same dates) as \code{daily.precip}.
#'
#' @param average.rainfall Reference value for average annual rainfall.
#'
#' @return A data frame with columns kbdi and drought.
#'
#' @export
#'
bom_db_drought <- function(daily.precip,
                           daily.tmax,
                           average.rainfall) {

  if (length(daily.precip) != length(daily.tmax)) {
    stop("Input vectors for precipitation and maximum temperature differ in length")
  }

  Ndays <- length(daily.precip)
  Kday <- numeric(Ndays)
  ET <- numeric(Ndays)

  daily.precip.eff <- bom_db_effective_rainfall(daily.precip)

  # Term 3 in ET equation, annual rainfall influence
  ET3 <- 1 + 10.88 * exp(-.001736 * average.rainfall)

  # Term 4 in ET equation
  ET4 <- 1e-3

  # First step requires two days of records.
  # Set to maximum dryness value
  Kday[1:2] <- 203.2

  # Helper for KBDI loop below
  clampK <- function(x) min(203.2, max(0, round(x, digits = 1)))

  for (i in 3:Ndays) {
    Kprev <- Kday[i-1]

    if (is.na(Kprev)) {
      # [Hamish] Missing Tmax or precip.eff values result in all subsequent
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
    ET2 <- 0.968 * exp(0.0875 * daily.tmax[i-1] + 1.5552) - 8.30
    ET[i] <- ((ET1 * ET2) / ET3) * ET4

    Kday[i] <- clampK( Kprev - daily.precip.eff[i] + ET[i] )
  }


  # Calculate drought factor values.
  # DF is based on the following over the last 20 days:
  #   E - rainfall event, defined as consec days each with rain > 2mm
  #   P - past rainfall amount i.e. sum over event E
  #   N - days since day of largest rainfall within event
  DFday <- numeric(Ndays)
  DFday[1:20] <- NA

  # For drought factor, only rainfall of 2mm or more
  # is considered
  prDF <- daily.precip
  prDF[ daily.precip <= 2 ] <- 0

  for (i in 21:Ndays) {
    if (is.na(Kday[i]) || is.na(daily.precip[i])) {
      DFday[i] <- NA

    } else {
      # Find most recent rainfall event not more than 20 days ago
      ii <- (i-1):(i-20)

      r20 <- prDF[ii]
      r20.flag <- prDF[ii] > 0

      # Look for instances of two consecutive days with rain
      is.event <- r20.flag & lead(r20.flag)

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

      if (Kday[i] >= 20) {
        xlim <- 75 / (270.525 - 1.267 * Kday[i])
      } else {
        xlim <- x
      }

      x <- min(xraw, xlim)
      xfactor <- (41 * x^2 + x) / (40 * x^2 + x + 1)

      DFbase <- 10.5* (1.0 - exp(-(Kday[i] + 30) / 40))
      DFraw <- DFbase * xfactor

      # If DFraw is NA, allow this to propagate to DFday
      DFday[i] <- round( min(10, DFraw), digits = 1)
    }
  }

  data.frame(kbdi = Kday, drought = DFday)
}


#' Calculate effective rainfall values from daily values
#'
#' For KBDI calculation (Finkele et al. 2006), the first 5mm of rain
#' do not count towards evapo-transpiration. If it rains less than 5mm over several days,
#' a running balance is kept. Once this balance exceeds 5mm, the first 5mm
#' is removed for canopy interception/runoff and the remainder
#' constitutes effective rainfall. Any day without rain resets the balance to zero.
#'
#' @param precip.daily Vector of daily precipitation values.
#'
#' @return A vector of effective rainfall values.
#'
#' @export
#'
bom_db_effective_rainfall <- function(precip.daily) {
  Ndays <- length(precip.daily)
  peff <- numeric(Ndays)
  bal <- 0

  for (i in 2:Ndays) {
    bal.prev <- bal

    if (precip.daily[i] == 0) {
      # reset for a day with no rain
      bal <- 0
    } else {
      if (bal.prev == 0) {
        # some rain this day and
        # none previously
        if (precip.daily[i] > 5) {
          # remove interception/runoff then set as Peff
          peff[i] <- precip.daily[i] - 5
          # Set bal to 5 so any rain in next day is added
          # without removing 5
          bal <- 5
        } else {
          # no effective rain but the balance increases
          bal <- precip.daily[i]
        }
      } else if (bal.prev < 5) {
        # some rain this day and
        # less than five previously
        if (precip.daily[i] + bal.prev > 5) {
          # remove interception/runoff, then set as Peff
          peff[i] <- precip.daily[i] + bal.prev - 5
          # Set bal to 5 so any rain in next day is added
          # without removing 5
          bal <- 5
        } else {
          # still haven't exceeded 5mm
          bal <- precip.daily[i] + bal.prev
        }
      } else {
        # interception/runoff has already been removed
        # so add all rain to Peff keep balance at 5
        peff[i] <- precip.daily[i]
        bal <- 5
      }
    }
  }

  peff
}
