test_that("A new db has all tables", {
  path <- ":memory:"
  db <- bom_db_create(path)
  tbls <- pool::dbListTables(db)
  pool::poolClose(db)

  expect_setequal(tolower(tbls), c("aws", "synoptic", "stations"))
})


test_that("Missing AWS table causes an error", {
  path <- tempfile("testdb", fileext = ".db")
  db <- bom_db_create(path)
  pool::dbExecute(db, "drop table AWS;")
  pool::poolClose(db)

  expect_error(bom_db_open(path), regexp = "missing")
})


test_that("Missing Synoptic table causes an error", {
  path <- tempfile("testdb", fileext = ".db")
  db <- bom_db_create(path)
  pool::dbExecute(db, "drop table Synoptic;")
  pool::poolClose(db)

  expect_error(bom_db_open(path), regexp = "missing")
})


test_that("Missing Stations table is created and filled", {
  path <- tempfile("testdb", fileext = ".db")
  db <- bom_db_create(path)
  pool::dbExecute(db, "drop table Stations;")
  pool::poolClose(db)

  testthat::expect_message({db <- bom_db_open(path)}, "Adding Stations")

  xcols <- pool::dbListFields(db, "Stations")
  expect_equivalent(xcols, colnames(CERMBweather::STATION_METADATA))

  res <- pool::dbGetQuery(db, "select count(station) as n from Stations;")
  expect_equal(res$n, nrow(CERMBweather::STATION_METADATA))

  pool::poolClose(db)
})
