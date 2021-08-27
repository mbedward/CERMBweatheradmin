###### Private (non exported) functions and variables

# Checks that an object is a valid database connection (pool object) and
# that the expected weather data tables are present
.ensure_connection <- function(db) {
  if (!CERMBweather::bom_db_is_connected(db)) {
    stop("Database connection is closed or invalid")
  }

  tbls <- tolower(DBI::dbListTables(db))
  Expected <- c("aws", "synoptic", "stations", "upperair")
  found <- Expected %in% tbls

  if (any(!found)) {
    # First close connection to avoid a memory leak
    if (inherits(db, "Pool")) pool::poolClose(db)
    else if (inherits(db, "DBIConnection")) DBI::dbDisconnect(db)

    msg1 <- ifelse(sum(!found) == 1, "table", "tables")
    msg2 <- paste(Expected[!found], collapse=", ")
    msg <- glue::glue("Database is missing {msg1} {msg2}.
                      The connection has been closed.")

    stop(msg)
  }
}


.is_pool_connection <- function(db) {
  inherits(db, "Pool")
}


.is_dbi_connection <- function(db) {
  inherits(db, "DBIConnection")
}


# Determine if the database connection is SQLite or PostgreSQL
# and issue an error if it is anything else.
.get_db_type <- function(db) {
  # Is it a pool object?
  if (.is_pool_connection(db)) {
    con.type <- tolower( pool::dbGetInfo(db)$pooledObjectClass )
    con.type <- stringr::str_trim(con.type)

  # Is it a DBIConnection object?
  } else if (.is_dbi_connection(db)) {
    con.type <- tolower( class(db) )
    con.type <- stringr::str_trim(con.type)
  }

  else {
    stop("Connection object is not a pool or DBIConnection")
  }

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
