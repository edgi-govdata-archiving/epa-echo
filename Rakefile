require 'csv'
require 'fileutils'
require 'json'
require 'set'

require 'fastcsv'
require 'open-uri/cached'
require 'pupa'

# The Environment Protection Agency Enforcement and Compliance History Online
# (ECHO) Database retrieves JSON from https://echo.epa.gov/app/proxy/proxy.php
# The API's parameters were discovered using the browser's web console. The
# possible parameter values were in other files loaded by the results page.

def client
  # Use a cache in development to avoid repeat requests.
  @client ||= Pupa::Processor::Client.new(cache_dir: '_cache', expires_in: 604800, level: 'INFO') # 1 week
end

def logger
  @logger ||= Pupa::Logger.new('eotarchive-echo')
end

desc 'Downloads CSV files of facilities from EPA ECHO Facility Search'
task :facility_list do
  FileUtils.mkdir_p('downloads')

  consts = {}

  # Get the column IDs, which normally populate the inputs' values in the
  # "Customize Columns" dialog at https://echo.epa.gov/facilities/facility-search/results
  data = open('https://echo.epa.gov/app/facility_search/results_columns_ALL.php').read
  consts[:qcolumns] = Set.new(JSON.load(data)['TableColumns'].map{|column| column.fetch('ServiceID')})
  # NOTE: The original query includes column IDs that don't appear in the
  # "Customize Columns" dialog. We include these mysterious column IDs anyway.
  consts[:qcolumns] += [70, 87, 104, 116, 138]
  consts[:qcolumns] = consts[:qcolumns].to_a.join(',')

  # Get the possible values for different parameters.
  options = {}

  # Get the state IDs, which normally populate the options' values for the
  # `#state` select input at https://echo.epa.gov/facilities/facility-search
  data = open('https://echo.epa.gov/app/scripts/facility_search/search_lookups.js').read
  # Drop the first item in the list, which is "Any".
  options[:p_st] = data.match(/\bstateArray = \[(.+?)\]/m)[1].scan(/"(.+?)"/).drop(1).flatten.map do |value|
    value.split(' - ', 2)[0]
  end

  # Get the county IDs, which normally populate the options' values for the
  # `#county` select input at https://echo.epa.gov/facilities/facility-search
  data = open('https://echo.epa.gov/app/scripts/county_codes.js').read
  # Build the list of county IDs for each state ID.
  options[:p_fips] = {}
  counties = data.match(/\bcountyCodesArray = \[(.+?)\]/m)[1].scan(/"(.+?)"/).flatten
  end_index = 0
  # Drop the first item in the list, which corresponds to "Any".
  data.match(/\bcountiesByStateArray = \[(.+?)\]/m)[1].scan(/"(.+?)"/).drop(1).flatten.each_with_index do |value, i|
    length = value[/\d+/].to_i
    end_index += length
    options[:p_fips][options[:p_st][i]] = counties[end_index - length + 1...end_index].map{|county| county.split(' - ', 2)[0]}
  end

  # Sends a search request to the API and downloads the corresponding CSV.
  #
  # @param [Hash] parameters The POST parameters
  # @param [Hash] options The possible values for each parameter
  # @param [Hash] consts Constant values for some parameters
  # @option options [Array<String>] :p_st The possible values of the `p_st` ("State") parameter
  # @option options [Array<String>] :p_fips The possible values of the `p_fips` ("County") parameter
  def post(parameters, options, consts)
    begin
      response = client.post('https://echo.epa.gov/app/proxy/proxy.php', parameters)

      if response.status == 200
        results = response.body.fetch('Results')
        if results.key?('Error')
          error_message = results['Error'].fetch('ErrorMessage')
          if error_message.match(/\ARows Returned would be \d+\.  Queryset Limit would be exceeded - please make search parmeters more selective!\z/)
            # If too many rows are returned, add a "County" parameter.
            # Needed for p_st=CA, FL, IL, MN, NJ, NY, TX.
            if !parameters.key?(:p_fips)
              options[:p_fips][parameters[:p_st]].each do |v|
                post(parameters.merge(p_fips: v), options, consts)
              end
            else
              logger.warn("#{response.status}: #{error_message}")
            end
          else
            # NOTE: The following spurious error occurs intermittently:
            # "Invalid character in callback function name - only alphabetic and numeric characters are allowed."
            logger.warn("#{response.status}: #{error_message}")
          end
        else
          response = client.post('https://ofmpub.epa.gov/echo/echo_rest_services2.get_download', {
            qid: results.fetch('QueryID'),
            # NOTE: When resubmitting a query, the application appends the new
            # column IDs, resulting in duplicate column IDs. This code doesn't.
            qcolumns: consts.fetch(:qcolumns),
          })

          basename = [:p_st, :p_fips].map{|parameter| parameters[parameter]}.compact.join('-')
          
          File.open(File.join('downloads', "#{basename}.csv"), 'w') do |f|
            f.write(response.body)
          end
        end
      else
        logger.warn("#{response.status}: #{response.body}")
      end
    rescue Faraday::ClientError => e
      logger.error("#{e.class} #{e.message} #{e.response}")
    end
  end

  # https://echo.epa.gov/app/proxy/proxy.php limits the total number of results
  # returned. We first limit by "State", then by "County".
  options[:p_st].each do |v|
    parameters = {
      s: 'fac', # required parameter, default is "fac" ("All Data")
      responseset: 0, # number of results to return, default is 500
      p_st: v,
    }

    post(parameters, options, consts)
  end
end

# Yields one facility ID at a time, to avoid reading a whole CSV into memory.
#
# @param [Array<String>] id_columns The columns that may contain the facility ID
# @param [Hash] options
# @option options [Boolean] :required Whether the facility must have an ID
# @option options [Boolean] :multiple Whether the facility may have multiple IDs
def facility_ids(id_columns, options = {})
  Pupa::Processor::Yielder.new do
    Dir[File.join('downloads', '*.csv')].each do |csv|
      print '.'

      id_column = nil
      FastCSV.foreach(csv, encoding: 'iso-8859-1', headers: true) do |row|
        id_column ||= id_columns.find{|id_column| row.key?(id_column)}

        if id_column
          id = row[id_column]

          if id
            if options[:multiple]
              id.split(' ').each do |v|
                Fiber.yield(v)
              end
            else
              Fiber.yield(id)
            end
          elsif options[:required]
            raise "#{csv}: expected non-empty '#{id_column}': #{row.inspect}"
          end
        else
          message = "#{csv}: expected one of '#{id_columns.join(', ')}' in #{row.headers.join(', ')}"
          if options[:required]
            raise message
          else
            logger.warn(message)
          end
          break
        end
      end
    end
  end
end

# Writes a file of URLs.
#
# @param [String] filename The file to create
# @param [Array<String>] id_columns The columns that may contain the facility ID
# @param [String] format_string The URL into which to subtitute the ID
# @param [Hash] options
def write_url_list(filename, id_columns, format_string, options = {})
  FileUtils.mkdir_p('urls')

  File.open(File.join('urls', filename), 'w') do |f|
    facility_ids(id_columns, options).to_enum.each do |id|
      f.write("#{format_string % {id: id}}\n")
    end
  end
end

# @see https://echo.epa.gov/help/facility-search/search-results-reports-legend
# C: Detailed Facility Report, e.g. https://echo.epa.gov/detailed-facility-report?fid=110000491735
# I: Civil Enforcement Case Report, e.g. https://echo.epa.gov/enforcement-case-report?id=10-1997-0156
# A: Air Pollutant Report, e.g. https://echo.epa.gov/air-pollutant-report?fid=110000491735
# E: Effluent Charts, e.g. https://echo.epa.gov/effluent-charts#NHG250392
# L: Discharge Monitoring Report, e.g. https://cfpub.epa.gov/dmr/facility_detail.cfm?fac=NHG250392

# TODO:
# X: Effluent Limit Exceedances Report
# W: CWA Program Area Reports
# D: Facility Documents

desc 'Create a file of URLs to JSON files of Detailed Facility Reports from EPA ECHO Facility Search'
task :detailed_facility_report_urls do
  write_url_list(
    'detailed_facility_report_urls.txt',
    ['registry_id', 'pwsid'],
    'https://echo.epa.gov/app/proxy/proxy.php?s=dfr&p_id=%<id>s',
    required: true)
end

desc 'Create a file of URLs to JSON files of Civil Enforcement Case Reports from EPA ECHO Facility Search'
task :civil_enforcement_case_report_urls do
  write_url_list(
    'civil_enforcement_case_report_urls.txt',
    ['fec_case_ids', 'air_case_ids', 'rcra_case_ids'],
    'https://echo.epa.gov/app/proxy/proxy.php?s=ecr&p_id=%<id>s',
    multiple: true)
end

desc 'Create a file of URLs to JSON files of Air Pollutant Reports from EPA ECHO Facility Search'
task :air_pollutant_report_urls do
  write_url_list(
    'air_pollutant_report_urls.txt',
    ['registry_id', 'pwsid'],
    'https://echo.epa.gov/app/proxy/proxy.php?s=caapr&p_id=%<id>s',
    required: true)
end

desc 'Create a file of URLs to JSON files of Effluent Charts from EPA ECHO Facility Search'
task :effluent_chart_urls do
  write_url_list(
    'effluent_chart_urls.txt',
    ['npdes_ids'],
    'https://ofmpub.epa.gov/echo/eff_rest_services.get_summary_chart?output=JSON&p_id=%<id>s',
    multiple: true)
end

desc 'Create a file of URLs to JSON files of Discharge Monitoring Reports from EPA ECHO Facility Search'
task :discharge_monitoring_report_urls do
  # TODO: Multiple years available.
  write_url_list(
    'discharge_monitoring_report_urls.txt',
    ['npdes_ids'],
    'https://cfpub.epa.gov/dmr/facility_detail.cfm?fac=%<id>s',
    multiple: true)
end
