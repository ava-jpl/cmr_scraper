## CMR Scraper
Metadata scraper for the EarthData CMR
----
There are 3 associated jobs:
- CMR - Scrape AOI
- CMR - Scrape AOI Timeframe
- CMR - Scrape User Region

### CMR - Scrape AOI
-----
Job is of type iteration. It takes in an input AOI, and queries the CMR for all time for the given product type over the spatial extent of the AOI. Publishes all results that do not exist on GRQ.

### Scrape AOI Timeframe
-----
Job is of type iteration. It takes in an input AOI, and queries the CMR for all time for the given product type over the input time region. Publishes all results that do not exist on GRQ.

### Scrape User Region
-----
Job is of type individual. Takes the user input of starttime, endtime, and geospatial extent, and queries the CMR for the given product type. Publishes all results that do not exist on GRQ.

MET-<short_name>-<start_time>_<end_time>-<version>

