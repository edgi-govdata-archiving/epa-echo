# EPA ECHO Scrapers

Scrapes some data from https://echo.epa.gov/

    bundle
    bundle exec rake --tasks

## Scrapers

Downloads the CSV data made available from [Facility Search Results](https://echo.epa.gov/facilities/facility-search) via its "Download CSV File" button.

    bundle exec rake facility_list

Downloads the JSON data made available on each Detailed Facility Report, linked from [Facility Search Results](https://echo.epa.gov/facilities/facility-search).

    bundle exec rake facility_details

Create a file with the URLs to the JSON data:

    bundle exec rake facility_detail_urls

Copyright (c) 2016 James McKinney, released under the MIT license
