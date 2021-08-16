###### Private (non exported) functions and variables

# Checks that an object is a valid database connection (pool object) and
# that the expected weather data tables are present
.ensure_connection <- function(db) {
  if (!.is_connection(db)) stop("Object is not a database connection pool")

  if (!pool::dbIsValid(db))
    stop("Database connection pool has been closed or was not initialized properly")

  # If the R session has been restarted, dbIsValid will return TRUE even
  # though the pool is effectively closed. As a work-around we do a quick
  # sniff query...
  tryCatch(pool::dbListTables(db),
           error = function(e) {
             stop("Database connection pool is not valid, e.g. due to an R restart")
           })

  tbls <- tolower(pool::dbListTables(db))
  Expected <- c("aws", "synoptic", "stations")
  found <- Expected %in% tbls

  if (any(!found)) {
    # First close connection to avoid a leaked pool object
    pool::poolClose(db)

    msg1 <- ifelse(sum(!found) == 1, "table", "tables")
    msg2 <- paste(Expected[!found], collapse=", ")
    msg <- glue::glue("Database is missing {msg1} {msg2}")
    stop(msg)
  }
}


.is_connection <- function(db) {
  cl <- inherits(db, c("Pool", "R6"), which = TRUE)
  all(cl == c(1,2))
}


.is_open_connection <- function(db) {
  .is_connection(db) && pool::dbIsValid(db)
}


# Determine if the database connection is SQLite or PostgreSQL
# and issue an error if it is anything else.
.get_db_type <- function(db) {
  con.type <- pool::dbGetInfo(db)$pooledObjectClass
  con.type <- tolower(con.type)
  con.type <- stringr::str_trim(con.type)

  if (con.type == "sqliteconnection") {
    "sqlite"
  } else if (con.type == "pqconnection") {
    "postgresql"
  } else if (con.type == "postgresqlconnection") {
    stop("Please use the 'RPostgres' package instead of 'RPostgreSQL'")
  } else {
    stop("Unsupported connection type: ", con.type)
  }
}

