# Mapped CloudStor group directory as Z drive
path <- "z:/Shared/NSW Bushfire Hub/Existing_Data/Meteorology/aws_synoptic/aws_metadata"

files <- dir(path, pattern = "StnDet", full.names = TRUE)

METADATA_AWS <- lapply(files, read.csv, header = FALSE, stringsAsFactors = FALSE)

METADATA_AWS <- do.call(rbind, METADATA_AWS)

ii.cols <- c(2, 4, 5, 7, 8, 9, 10, 11, 12)
ii.names <- c("station", "name", "start.date", "lat", "lon", "loc.method", "state", "elev1", "elev2")
colnames(METADATA_AWS)[ii.cols] <- ii.names
METADATA_AWS <- METADATA_AWS[, ii.cols]

METADATA_AWS <- METADATA_AWS %>%
  dplyr::mutate(
    start.month = as.integer(stringr::str_extract(start.date, "^\\d{1,2}")),
    start.year = as.integer(stringr::str_extract(start.date, "\\d+$"))) %>%

  dplyr::select(station, name, state, lat, lon, loc.method,
                elev1, elev2, start.month, start.year) %>%

  dplyr::arrange(station)


usethis::use_data(METADATA_AWS, overwrite = TRUE)
