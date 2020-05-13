#' Lookup table for station data fields
#'
#' This data frame is used to map input weather station data columns
#' to fields in database tables.
#'
"COLUMN_LOOKUP"

#' SQL statements to create database tables
#'
#' A list of SQL statements used to create the AWS, Synoptic and Stations
#' tables in a new database.
#'
#' @seealso \code{\link{bom_db_create}}
#'
"SQL_CREATE_TABLES"

#' Weather station metadata
#'
#' A data frame that, for each BOM weather station, gives identifying
#' number, name (locality), start of operation (month and year), state
#' (including 'ANT' for Antarctica), longitude, and latitude. Also
#' includes average annual precipitation value for each station based
#' on the NARCLIM p12 layer.
#'
"STATION_METADATA"
