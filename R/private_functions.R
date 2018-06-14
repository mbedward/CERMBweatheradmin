###### Private (non exported) functions and variables

# Checks that an object is a valid database connection (pool object) and
# that the expected weather data tables are present
.ensure_connection <- function(x) {
  if (!.is_connection(x)) stop("Object is not a database connection pool")

  if (!DBI::dbIsValid(x))
    stop("Database connection pool has been closed or was not initialized properly")

  if (!.is_weather_db(x))
    stop("Database is missing one or both of AWS and Synoptic tables")
}


.is_connection <- function(x) {
  cl <- inherits(x, c("Pool", "R6"), which = TRUE)
  all(cl == c(1,2))
}


.is_open_connection <- function(x) {
  .is_connection(x) && DBI::dbIsValid(x)
}


# Checks that AWS and Synoptic tables are present
.is_weather_db <- function(x) {
  tbls <- DBI::dbListTables(x)
  all(c("synoptic", "aws") %in% tolower(tbls))
}


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


.is_zip_file <- function(paths) {
  path <- stringr::str_trim(paths)
  stringr::str_detect(tolower(paths), "\\.zip$")
}


.is_directory <- function(paths) {
  file.info(paths)$isdir
}


.get_file_name <- function(paths) {
  x <- stringr::str_split(paths, "[\\\\/]+")
  sapply(x, tail, 1)
}


# guards against double or mixed slashes in constructed paths
# (there must be a more elegant alternative)
.safe_file_path <- function(...) {
  x <- file.path(...)
  x <- stringr::str_split(x, "[\\\\/]+")
  sapply(x, stringr::str_c, collapse = "/")
}


.extract_station_numbers <- function(filenames) {
  x <- stringr::str_extract(filenames, "_Data_\\d+")
  stringr::str_extract(x, "\\d+")
}
