#' Summarizes weather station files in a directory
#'
#' This function summarizes the contents of a directory containing delimited
#' text data files for one or more weather stations.
#'
#' @param path Path to the input directory.
#'
#' @param include One of the following (may be abbreviated):
#'   'data' (default) to only list stations with non-empty data sets;
#'   'all' to list all stations;
#'   'empty' to only list stations with empty data sets.
#'
#' @return A data frame with columns station (identifier); filename; filesize.
#'
#' @export
#'
bom_dir_summary <- function(path, include = c("data", "all", "empty")) {

  include <- match.arg(include)

  # Identify station data files
  files <- dir(path, full.names = FALSE)
  files <- stringr::str_subset(files, "^.+_Data_[_\\d]+\\.txt$")

  filepaths <- sapply(files, function(f) .safe_file_path(path, f))

  sizes <- file.size(filepaths)

  station <- CERMBweather::bom_db_extract_station_numbers(files)

  dat <- data.frame(station, filename = files, filesize = sizes,
                    stringsAsFactors = FALSE)

  ii <- switch(include,
               data = sizes > 0,
               all = rep(TRUE, length(files)),
               empty = sizes == 0)

  dat[ii, ]
}
