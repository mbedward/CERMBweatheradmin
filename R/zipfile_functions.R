#' Summarizes weather stations in a zipped data file
#'
#' This function summarizes the contents of a zip file containing data sets
#' for one or more weather stations.
#'
#' @param zipfile Path to the input zip file.
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
bom_zip_summary <- function(zipfile, include = c("data", "all", "empty")) {
  include <- match.arg(include)

  files <- unzip(zipfile, list = TRUE)

  if (length(files) == 0) {
    return( data.frame(station = integer(0), filename = character(0), filesize = integer(0)) )
  }

  # Identify station data files
  pattern <- "_Data_[_\\d]+\\.txt$"
  ii <- stringr::str_detect(files[["Name"]], pattern)

  ii <- switch(include,
               data = ii & files[["Length"]] > 0,
               all = ii,
               empty = ii & files[["Length"]] == 0)

  # Data file names
  filename <- files[["Name"]][ii]

  # Station IDs
  station <- .extract_station_numbers(filename)

  data.frame(station, filename, filesize = files[["Length"]][ii],
             stringsAsFactors = FALSE)
}


#' Reads raw data for specified weather stations from a zip file
#'
#' @param zipfile Path to the input zip file.
#'
#' @param station Station identifiers as integer station numbers or character strings.
#'
#' @param allow.missing If TRUE (default), the function will return NULL if the
#'   requested station is not found in the zip file. If FALSE, an error
#'   is thrown if the station is missing.
#'
#' @param out.format One of 'list' (default) to return a named list of data frames,
#'   or 'single' to return a single frame of data for all stations.
#'
#' @return If out.format is 'list', a named list, where names are station
#'   identifiers (character strings of length six) and elements are data frames
#'   of station data. Each data frame has an attribute 'datatype' with value 'synoptic'
#'   or 'aws'. If out.format is 'single', a combined data frame of data for all
#'   stations, with an attribute 'datatype'.
#'
#' @export
#'
bom_zip_data <- function(zipfile, stations,
                         allow.missing = TRUE,
                         out.format = c("list", "single")) {

  out.format <- match.arg(out.format)

  stn.info <- bom_zip_summary(zipfile)

  id <- bom_station_id(stations)

  ii <- match(id, stn.info[["station"]])
  if (anyNA(ii)) {
    if (allow.missing) {
      if (all(is.na(ii)))
        return(NULL)  # TODO better option?
      else
        ii <- na.omit(ii)

    } else {
      # missing stations not allowed
      misses <- stations[is.na(ii)]
      stop("No data found for the following station(s)\n", misses)
    }
  }

  filenames <- stn.info[["filename"]][ii]

  res <- lapply(filenames,
                function(fname) {
                  zcon <- unz(zipfile, fname)
                  dat <- read.csv(zcon, stringsAsFactors = FALSE)
                  .map_fields(dat)
                })

  if (out.format == "list") {
    names(res) <- stn.info[["station"]][ii]
  }
  else { # out.format == "single"
    # Note: we assume all data is of the same type, synoptic or AWS
    type <- attr(res[[1]], "datatype")
    res <- dplyr::bind_rows(res)
    attr(res, "datatype") <- type
  }

  res
}

