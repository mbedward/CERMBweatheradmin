# CERMBweatheradmin

A package of functions to help manage a PostgreSQL database of weather station
data provided by the Australian Bureau of Meteorology (BOM). The package is 
developed for use by the Centre for Environmental Risk Management of Bushfires,
University of Wollongong (Australia). It contains functions to import records
from BOM synoptic and AWS (automatic weather station) data files.

Early versions of the package worked with a SQLite database. For the moment the
SQLite-related content has been replaced by PostgreSQL functions, but the SQLite
code can be found in the git history and might be ressurrected at some stage.

A related R package with functions to inspect and tidy BOM data files (CSV and 
zipped formats) and compose some handy database queries can be found at 
https://github.com/mbedward/CERMBweather

Yet another related R package with functions to calculate fire-related variables
from weather station records, including FFDI (Forest Fire Danger Index), can be
found at https://github.com/mbedward/CERMBffdi

**Note:** Please feel free to look around, and to use or adapt the code here.
However, keep in mind that the functions in this package currently assume a
particular database structure, as well as data formatting conventions that are
specific to the Australian Bureau of Meteorology. If you would like to know more
about the database structure, see the function `db_creation_script` that writes
all of the SQL commands needed to create the database tables and views to a
file.

```
# Install the remotes package if not already present
install.packages("remotes")

remotes::install_github("mbedward/CERMBweatheradmin")

```
