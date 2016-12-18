# Environment Protection Agency (EPA) Enforcement and Compliance History Online (ECHO) Database Scrapers

Scrapes some data from https://echo.epa.gov/

**Note:** Some or all of this data may already be available as bulk downloads at https://echo.epa.gov/tools/data-downloads#downloads

    bundle
    bundle exec rake --tasks

## Scrapers

This command downloads the CSV data made available from [Facility Search Results](https://echo.epa.gov/facilities/facility-search) via its "Download CSV File" button.

    bundle exec rake facility_list

The CSV files in the `downloads/` directory are named after the parameters used in the queries that generate them and follow the pattern `XX-DDDDD.csv`, where `XX` is the "State" parameter value and `DDDDD` is the "State" parameter value.

These commands create a file with the URLs to the JSON data for:

* Detailed Facility Reports:

    bundle exec rake detailed_facility_report_urls

* Air Pollutant Reports:

    bundle exec rake air_pollutant_report_urls

* Civil Enforcement Case Reports:

    bundle exec rake civil_enforcement_case_report_urls

* Effluent Charts

    bundle exec rake effluent_chart_urls

* Discharge Monitoring Reports

    bundle exec rake discharge_monitoring_report_urls

Copyright (c) 2016 James McKinney, released under the MIT license
