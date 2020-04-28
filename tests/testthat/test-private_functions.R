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
  tryCatch(
    expect_error( fn(DB), regexp = "missing.*tables" ),
    finally = pool::poolClose(DB)
  )
})


test_that("ymd to date", {
  fn <- CERMBweather:::.ymd_to_date

  expect_equal(as.Date("2020-04-23"), fn(2020, 4, 23))

  years <- c(rep(1999, 31), rep(2000, 61))
  months <- c(rep(12, 31), rep(1, 31), rep(2, 29), 3)
  days <- c(1:31, 1:31, 1:29, 1)

  expected <- seq(as.Date("1999-12-01"), as.Date("2000-03-01"), by = "1 day")
  expect_equal(expected, fn(years, months, days))
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


test_that("guess data type with only zero rainfall", {
  fn <- CERMBweather:::.guess_data_type

  dat <- expand.grid(
    year = 2020,
    month = 4,
    day = 1:30,
    hour = seq(0, 21, by = 3),
    minute = 0,
    precipitation = 0)

  expect_equal(fn(dat), "synoptic")

  dat$windgust <- 0
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

