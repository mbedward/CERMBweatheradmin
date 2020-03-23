#' Calculate FFDI for specified weather stations
#'
#' @param db A database connection pool object as returned by
#'   \code{\link{bom_db_open}} or \code{\link{bom_db_create}}.
#'
#' @param stations A vector of one or more integer weather station identifiers.
#'
#' @export
#'
calculate_station_ffdi <- function(db, stations,
                                   av.rainfall.years = 2001:2015,
                                   min.days.per.year = 300) {

  res <- lapply(stations, function(stn) {
    dat <- bom_db_synoptic(db) %>%
      dplyr::filter(station == stn) %>%
      dplyr::collect()

    if (nrow(dat) > 0) {
      .do_station_ffdi(dat, av.rainfall.years, min.days.per.year)
    } else {
      warning("No records for station ", stn, immediate. = TRUE)
      NULL
    }
  })

  dplyr::bind_rows(res)
}


.do_station_ffdi <- function(dat.stn, av.rainfall.years, min.days.per.year) {
  dat.stn <- dat.stn %>%
    # Ensure only one record per time point (this will arbitrarily
    # take the first from any duplicate set)
    dplyr::distinct(year, month, day, hour, minute, .keep_all = TRUE) %>%

    # Treat missing rainfall values as zero (least worst option)
    dplyr::mutate(precipitation = ifelse(is.na(precipitation), 0, precipitation))


  # Calculate average annual rainfall over the reference years
  x <- dat.stn %>%
    dplyr::filter(year %in% av.rainfall.years) %>%
    dplyr::group_by(year) %>%
    dplyr::summarize(ndays = dplyr::n_distinct(month, day),
                     precipitation.year = sum(precipitation))

  if (all(av.rainfall.years %in% x$year) & all(x$ndays >= min.days.per.year)) {
    AvRainfall <- mean(x$precipitation.year)

  } else {
    warning("Unable to calculate average annual rainfall for station ",
            dat.stn$station[1],
            immediate. = TRUE)

    return(NULL)
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

    dplyr::mutate(kbdi = calculate_daily_kbdi(precip.daily, tmax.daily, AvRainfall) )


  dat.stn <- dat.stn %>%
    # Join daily value of KBDI to original time-step data
    dplyr::left_join(dat.daily, by = c("year", "month", "day")) %>%

    # Add calculated FFDI values and return the data frame.
    # Missing values in any of the input variables will propagate
    # through to FFDI auto-magically.
    dplyr::mutate(ffdi = round(
      2 * exp(0.987 * log(kbdi) - 0.45 +
                0.0338 * temperature +
                0.0234 * windspeed -
                0.0345 * relhumidity),
      digits = 1))

  dat.stn
}


#' Calculate the Keetch-Byram drought index (soil moisture deficit)
#'
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
#' @return A vector of KBDI values of the same length as the input precipitation
#'   and temperature vectors.
#'
#' @export
#'
calculate_daily_kbdi <- function(daily.precip,
                                 daily.tmax,
                                 average.rainfall) {

  if (length(daily.precip) != length(daily.tmax)) {
    stop("Input vectors for precipitation and maximum temperature differ in length")
  }

  Ndays <- length(daily.precip)
  Kday <- numeric(Ndays)
  ET <- numeric(Ndays)

  daily.precip.eff <- calculate_effective_rainfall(daily.precip)

  # Term 3, annual rainfall influence
  ET3 <- 1+10.88*exp(-.001736 * average.rainfall)

  # Term 4 in ET equation
  ET4 <- 1.e-3

  # First step requires two days of records.
  # Set to maximum dryness value
  Kday[1:2] <- 203.2

  # Helper for loop below
  clamp <- function(x) min(203.2, max(0, round(x, digits = 1)))

  for (i in 3:Ndays) {
    # ET applies yesterday's max temp to yesterday's KDBI, adjusted
    # for mean annual rainfall, to increase the soil moisture deficit
    ET1 <- 203.2 - Kday[i-1]
    ET2 <- 0.968 * exp(0.0875 * daily.tmax[i-1] + 1.5552) - 8.30
    ET[i] <- ((ET1 * ET2) / ET3) * ET4

    if (is.na(daily.tmax[i]) || is.na(daily.precip[i])) {
      Kday[i] <- NA
    } else {
      Kday[i] <- clamp(Kday[i-1] - daily.precip.eff[i] + ET[i])
    }
  }

  # drought factor rain. Nonsense about last 20 days, etc.
  # An event can be multi-dayed, if over 2 mm each day.
  # In that case, the timing of the event is the day with the most rain.
  DFday <- numeric(Ndays)
  DFday[1:20] <- NA

  # start on day 21 because need 20 days before,
  for (i in 21:Ndays) {
    iiprev <- (i-20):(i-1)
    rain.window <- daily.precip.eff[iiprev]
    kb <- Kday[iiprev]

    curr.rain <- 0		# amount / P
    curr.x <- -999
    curr.age <- 0
    sig.rain <- 0
    sig.x <- -999
    sig.age <- 0
    lim.x <- 1
    most.rain <- 0

    for (j in 20:1) {
      # rain must exceed 2mm to qualify
      if (is.na(rain.window[j]) || rain.window[j] < 2) {
        most.rain <- 0
        curr.rain <- 0
        curr.age <- 20-j
        curr.x <- 1
      } else {
        curr.rain <- curr.rain + rain.window[j]
        if (rain.window[j] >= most.rain) {
          curr.age <- 20 - j
          most.rain <- rain.window[j]
        }
      }

      if (curr.rain > 2) {
        if (curr.age == 0) curr.age <- 0.8
        curr.x <- curr.age^1.3 / (curr.age^1.3 + curr.rain - 2)
      } else {
        curr.x <- 1
      }

      if (sig.rain > 2) {
        if (sig.age == 0) sig.age <- 0.8
        sig.x <- sig.age^1.3 / (sig.age^1.3 + sig.rain - 2)
      } else {
        sig.x <- 1
      }

      if (curr.x <= sig.x) {
        sig.rain <- curr.rain
        sig.x <- curr.x
        sig.age <- curr.age
      }

      if (kb[j] < 20) {
        lim.x <- 1 / (1 + 0.1135 * kb[j])
      } else {
        lim.x <- 75 / (270.525 - 1.267 * kb[j])
      }

      if (lim.x < sig.x) sig.x <- lim.x
    }

    factor1 <- (41 * sig.x^2 + sig.x) / (40 * sig.x^2 + sig.x + 1)

    Kyest <- Kday[i-1]
    DF.base <- 10.5 * (1 - exp(-(Kyest + 30) / 40) )
    DF.final1 <- DF.base * factor1

    DFday[i] <- round(min(10, DF.final1), digits = 1)
  }

  DFday
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
calculate_effective_rainfall <- function(precip.daily) {
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
