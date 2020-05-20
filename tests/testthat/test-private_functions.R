test_that("ensure database connection", {
  fn <- CERMBweather:::.ensure_connection

  DB <- bom_db_create(":memory:")
  expect_silent( fn(DB) )

  bom_db_close(DB)
  expect_error( fn(DB) )
})


test_that("ensure database connection checks for tables", {
  fn <- CERMBweather:::.ensure_connection

  # This should work
  DB <- bom_db_create(":memory:")
  tryCatch(
    expect_silent( fn(DB) ),
    finally = bom_db_close(DB)
  )

  # This should fail (no tables)
  DB <- pool::dbPool(RSQLite::SQLite(), dbname = ":memory:")
  expect_error( fn(DB), regexp = "missing.*tables" )
})


test_that("ymd to date with vectors", {
  fn <- CERMBweather:::.ymd_to_date

  expect_equal(as.Date("2020-04-23"), fn(2020, 4, 23))

  years <- c(rep(1999, 31), rep(2000, 61))
  months <- c(rep(12, 31), rep(1, 31), rep(2, 29), 3)
  days <- c(1:31, 1:31, 1:29, 1)

  expected <- seq(as.Date("1999-12-01"), as.Date("2000-03-01"), by = "1 day")
  expect_equal(fn(years, months, days), expected)
})


test_that("ymd to date with data frame", {
  fn <- CERMBweather:::.ymd_to_date

  dat <- data.frame(year = c(rep(1999, 31), rep(2000, 61)),
                    month = c(rep(12, 31), rep(1, 31), rep(2, 29), 3),
                    day = c(1:31, 1:31, 1:29, 1))

  expected <- seq(as.Date("1999-12-01"), as.Date("2000-03-01"), by = "1 day")

  expect_equal(fn(dat), expected)

  expect_equal(fn(as.matrix(dat)), expected)
})


test_that("ymd to date checks for equal vector lengths", {
  fn <- CERMBweather:::.ymd_to_date

  expect_error(fn(rep(2020, 31), 4, 1:31), regexp = "same length")
})


test_that("date to ymd", {
  fn <- CERMBweather:::.date_to_ymd

  dates <- as.Date("2020-04-23")
  expected <- data.frame(year = 2020, month = 4, day = 23)
  expect_equal(expected, fn(dates))

  dates <- seq(as.Date("1999-12-01"), as.Date("2000-03-01"), by = "1 day")
  expected <- data.frame(year = c(rep(1999, 31), rep(2000, 61)),
                         month = c(rep(12, 31), rep(1, 31), rep(2, 29), 3),
                         day = c(1:31, 1:31, 1:29, 1) )
  expect_equal(expected, fn(dates))
})


test_that("add missing days passes a complete time series", {
  dates <- seq(as.Date("2020-01-20"), as.Date("2020-02-10"), by = "1 day")
  dat <- CERMBweather:::.date_to_ymd(dates)
  dat$hour <- 0
  dat$minute <- 0

  res <- CERMBweather:::.add_missing_days(dat)
  expect_equivalent(res, dat)
  testthat::expect_length(attr(res, "dates.added"), 0)
})


test_that("add missing days with interrupted time series", {
  dates.all <- seq(as.Date("2020-01-20"), as.Date("2020-02-10"), by = "1 day")
  n <- length(dates.all)
  ii <- sample(2:(n-1), size = min(5, n-2))
  dates.lost <- dates.all[ii]
  dates.kept <- dates.all[-ii]

  dat <- CERMBweather:::.date_to_ymd(dates.kept)
  dat$hour <- 0
  dat$minute <- 0
  dat <- cbind(station = 123456, dat)

  dat$temperature <- sample(15:25, nrow(dat), replace = TRUE)
  dat$precipitation <- 0
  dat$relhumidity <- sample(50:100, nrow(dat), replace = TRUE)

  res <- CERMBweather:::.add_missing_days(dat)
  res.dates <- CERMBweather:::.ymd_to_date(res$year, res$month, res$day)

  # Day sequence should now be complete
  expect_setequal(res.dates, dates.all)

  # All records should have the same station number
  expect_true(all(res$station == 123456))

  # All other non-time cols for new records should be NA
  ii <- res.dates %in% dates.lost
  expect_true(all(is.na(res$temperature[ii])))
  expect_true(all(is.na(res$precipitation[ii])))
  expect_true(all(is.na(res$relhumidity[ii])))

  # Attribute should hold dates added
  x <- attr(res, "dates.added")
  expect_setequal(x, dates.lost)
})


test_that("find tail missing values in vector", {
  fn <- CERMBweather:::.find_na_tail

  x <- 1:20
  expect_true(is.na(fn(x)))

  x[1] <- NA
  expect_true(is.na(fn(x)))

  x[20] <- NA
  expect_equal(fn(x), 20)

  x[11:18] <- NA
  expect_equal(fn(x), 20)

  x[11:20] <- NA
  expect_equal(fn(x), 11)
})


test_that("guess data type aws with windspeed and windgust col", {
  fn <- CERMBweather:::.guess_data_type

  dat <- expand.grid(
    year = 2020,
    month = 4,
    day = 1,
    hour = 1,
    minute = 0,
    precipitation = 0,
    windspeed = 10,
    windgust = 42)

  expect_equal(fn(dat), "aws")
})

test_that("guess data type aws with sub-hourly time steps", {
  fn <- CERMBweather:::.guess_data_type

  dat <- expand.grid(
    year = 2020,
    month = 4,
    day = 1,
    hour = 1,
    minute = c(0, 15, 30, 45),
    precipitation = 0)

  expect_equal(fn(dat), "aws")
})


test_that("guess data type synoptic with non-ascending rainfall", {
  fn <- CERMBweather:::.guess_data_type

  dat <- expand.grid(
    year = 2020,
    month = 4,
    day = 1:30,
    hour = seq(0, 21, by = 3),
    minute = 0)

  dat$precipitation <- rep(c(0,5,0,1,0), length = nrow(dat))

  expect_equal(fn(dat), "synoptic")
})


test_that("require cols with and without case", {
  fn <- CERMBweather:::.require_columns

  dat <- data.frame(a = 1, b = 2, c = 3)

  expect_silent(fn(dat, c("a", "b", "c")))
  expect_silent(fn(dat, c("A", "b", "C"), ignore.case = TRUE))

  # default is case-sensitive matching
  testthat::expect_error(fn(dat, "B"), regexp = "not present.*B")
})


test_that("convert NA to zero in vector", {
  Ndata <- 100
  Nmiss <- 20

  ii <- sample(Ndata, Nmiss)
  x <- 1:Ndata
  x[ii] <- NA

  x2 <- CERMBweather:::.na2zero(x)

  expect_false(anyNA(x2))
  expect_equal(length(x2), Ndata)
  expect_equal(sum(x2 == 0), Nmiss)
})


test_that(".max_with_na", {
  fn <- CERMBweather:::.max_with_na

  x <- numeric(0)
  expect_equal(fn(x), NA)

  x <- rep(NA, 10)
  expect_equal(fn(x), NA)

  x[10] <- 42
  expect_equal(fn(x), 42)
})

