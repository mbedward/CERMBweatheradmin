# Query NARCLIM layer for mean annual precipitation at station locations
library(raster)

# Local copy of NARCLIM layer
path <- "s:/Biosci Bushfires/CERMB_LIBRARY/GIS/CLIMATE/NARCLIM/baseline_1990_2009_bioclim_p12.flt"

r.narclim <- raster(path)

# Many coastal stations (e.g. Kurnell, Green Cape) will fall just outside
# the NARCLIM data because of both the limited precision of the raster and
# the station locations. We bodge our way through this by putting a buffer
# around the coast and setting the value of buffer cells to the average of
# their data-cell neighbours. We iterate through this several times to build
# up a decent width buffer. This works but is not efficient - it takes
# about 1 minute per iteration)

# Matrix of uniform weights
w <- matrix(1, 3, 3)

r.narclim.buf <- raster::readAll(r.narclim)

for (i in 1:5) {
  cat("iteration", i, "\n")
  r.narclim.buf <- raster::focal(r.narclim.buf, w,
                                 fun = mean,
                                 na.rm = TRUE, pad = TRUE, NAonly = TRUE)
}

# Query raster at station locations (no interpolation). Some old stations
# with very imprecise coordinates will still be missed but we will live
# with that. We also will not have values for offshore islands or
# Antarctica (!)
#
data("STATION_METADATA")
lonlat <- STATION_METADATA[, c("lon", "lat")]
x <- raster::extract(r.narclim.buf, lonlat, method = "bilinear")

# Add rounded rainfall values to the station metadata and re-save
STATION_METADATA$annualprecip_narclim <- round(x)
usethis::use_data(STATION_METADATA, overwrite = TRUE)
