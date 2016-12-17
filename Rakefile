require 'json'
require 'set'

require 'open-uri/cached'
require 'pupa'

def client
  @client ||= Pupa::Processor::Client.new(cache_dir: '_cache', expires_in: 604800, level: 'INFO') # 1 week
end

def logger
  @logger = Pupa::Logger.new('archive')
end

desc 'Downloads CSVs of facilities from EPA ECHO Facility Search'
task :facilities do
  consts = {}

  # Get the column IDs, which populate the inputs' values in the "Customize
  # Columns" dialog at https://echo.epa.gov/facilities/facility-search/results
  data = open('https://echo.epa.gov/app/facility_search/results_columns_ALL.php').read
  consts[:qcolumns] = Set.new(JSON.load(data)['TableColumns'].map{|column| column.fetch('ServiceID')})
  # NOTE: The original query includes column IDs that don't appear in the
  # dialog. We include these mysterious column IDs anyway.
  consts[:qcolumns] += [70, 87, 104, 116, 138]
  consts[:qcolumns] = consts[:qcolumns].to_a.join(',')

  # Get the possible values for different parameters.
  options = {
    p_act: %w(Y N), # "Active"
  }

  # Get the state IDs, which populate the options' values for the `#state`
  # select input at https://echo.epa.gov/facilities/facility-search
  data = open('https://echo.epa.gov/app/scripts/facility_search/search_lookups.js').read
  # Drop the first item in the list, which is "Any".
  options[:p_st] = data.match(/stateArray\s*=\s*\[(.+?)\]/m)[1].scan(/"(.+?)"/).drop(1).map do |value|
    value[0].split(' - ', 2)[0]
  end

  # https://echo.epa.gov/app/scripts/facility_search/results.js translates the
  # options' values from the `#mediaSelect` select input at
  # https://echo.epa.gov/facilities/facility-search to a parameter value.
  data = open('https://echo.epa.gov/app/scripts/facility_search/results.js').read
  # Drop the last item in the list, which is the default "fac" ("All Data").
  options[:s] = data.scan(/'s=([^&']+)[^']*';/)[0...-1].map(&:first)

  # NOTE: When resubmitting a query, the application appends the new column IDs,
  # resulting in duplicate column IDs. This code doesn't submit duplicates.

  # @param [Hash] parameters The POST parameters
  # @param [Hash] options The possible values for each parameter
  # @param [Hash] consts Constant values for some parameters
  # @option options [Array<String>] :s The possible values of the `s` ("Search Type") parameter
  # @option options [Array<String>] :p_st The possible values of the `p_st` ("State") parameter
  def post(parameters, options, consts)
    begin
      response = client.post('https://echo.epa.gov/app/proxy/proxy.php', parameters)

      if response.status == 200
        results = response.body.fetch('Results')
        if results.key?('Error')
          error_message = results.fetch('Error').fetch('ErrorMessage')
          if error_message.match(/\ARows Returned would be \d+\.  Queryset Limit would be exceeded - please make search parmeters more selective!\z/)
            # If too many rows are returned, change the "Search Type" parameter.
            # Needed for p_st=CA, FL, IL, MN, NJ, NY, TX.
            if parameters[:s] == 'fac'
              options[:s].each do |v|
                post(parameters.merge(s: v), options, consts)
              end
            # If too many rows are still returned, add an "Active" parameter.
            # Needed for pst=NY and s=rcra ("Hazardous Waste").
            elsif !parameters.key?(:p_act)
              options[:p_act].each do |v|
                post(parameters.merge(p_act: v), options, consts)
              end
            else
              logger.warn("#{response.status}: #{error_message}")
            end
          else
            # NOTE: The following error occurs intermittently:
            # "Invalid character in callback function name - only alphabetic and numeric characters are allowed."
            logger.warn("#{response.status}: #{error_message}")
          end
        else
          response = client.post('https://ofmpub.epa.gov/echo/echo_rest_services2.get_download', {
            qid: results.fetch('QueryID'),
            qcolumns: consts.fetch(:qcolumns),
          })

          basename = [:p_st, :s, :p_act].map{|parameter| parameters[parameter]}.compact.join('-')
          
          File.open("#{basename}.csv", 'w') do |f|
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
  # returned. We first limit by "State", then by "Search Type".
  options[:p_st].each do |v|
    parameters = {
      s: 'fac', # required parameter, default is "fac" ("All Data")
      responseset: 0, # number of results to return, default is 500
      p_st: v,
    }

    post(parameters, options, consts)
  end
end
