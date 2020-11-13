# CERMBweatheradmin

A package of functions to help manage a PostgreSQL database of weather station
data provided by the Australian Bureau of Meteorology (BOM). The package is 
developed for use by the Centre for Environmental Risk Management of Bushfires,
University of Wollongong (Australia). It contains functions to import records
from BOM synoptic and AWS (automatic weather station) data files; and calculate
fire-weather variables such as Forest Fire Danger Index (FFDI).

Early versions of the package worked with a SQLite database. For the moment the
SQLite-related content has been replaced by PostgreSQL functions, but the SQLite
code can be found in the git history and might be ressurrected at some stage.

An associated R package with functions to perform common database queries
can be found at https://github.com/mbedward/CERMBweather

**Note** Feel free to look at or use the code but (a) lots of things don't
work yet, and (b) the functions here assume a particular database
structure and conventions specific to BOM weather station data.
