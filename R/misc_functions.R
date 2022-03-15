#' Write the database creation script to a file
#'
#' The package includes a copy of the SQL script used to create the
#' PostgreSQL/PostGIS database structure that is assumed by the
#' package functions. This function simply copies the script into
#' a specified text file so that you can browse the code or use it
#' to create your very own database.
#'
#' @param dest The destination path and filename. Defaults to
#'   \code{dbcreate.sql} in the current directory.
#'
#' @export
#'
db_creation_script <- function(dest = "dbcreate.sql") {
  ok <- file.copy(from = system.file("extdata", "create_cermb_weather_db.sql",
                                     package = "CERMBweatheradmin"),
                  to = dest,
                  overwrite = TRUE)

  if (ok) message("SQL script written to ", dest)
  else message("Problem with destination path")
}
