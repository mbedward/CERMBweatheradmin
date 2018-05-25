###### Private (non exported) functions and variables

.is_connection <- function(x) inherits(x, "SQLiteConnection")

.is_open_connection <- function(x) .is_connection(x) && RSQLite::dbIsValid(x)


.ensure_connection <- function(x, ...) {
  if (!.is_connection(x)) stop("Object is not a database connection")

  # Prepare flags variable
  flags <- 0
  for (dot in c(...)) flags <- bitwOr(flags, dot)

  if (flags > 0) {
    # Helper to check if a flag is set
    is_set <- function(flag) bitwAnd(flags, flag) > 0

    # If not checking that the connection is open, allow for
    # the possibility that it is closed.
    to.close <- FALSE
    if (!is_set(.CON_FLAGS$Open)) {
      if (!.is_open_connection(con)) {
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = con@dbname)
        to.close <- TRUE
      }
    }

    # Checking for data implies checking for tables
    if (is_set(.CON_FLAGS$HasData))
      flags <- bitwOr(flags, .CON_FLAGS$HasTables)


    # Ensure open
    if (is_set(.CON_FLAGS$Open)) {
      if (!DBI::dbIsValid(x))
        stop("Database connection is not open")
    }

    if (is_set(.CON_FLAGS$HasTables)) {
      if ( !(.db_has_table(con, "AWS") && .db_has_table(con, "Synoptic")) )
        stop("Database is missing one or both required tables AWS and Synoptic")
    }

    if (is_set(flags, .CON_FLAGS$HasData)) {
      nrecs <- bom_db_summary(con, by = "total")[["nrecs"]]
      if (!any(nrecs > 0)) stop("Database has no data records")
    }

    if (to.close) DBI::dbDisconnect(con)
  }
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


.db_has_table <- function(con, tblname) {
  if (!.is_open_connection(con))
    stop("Database connection is not open")

  tolower(tblname) %in% tolower(DBI::dbListTables(con))
}
