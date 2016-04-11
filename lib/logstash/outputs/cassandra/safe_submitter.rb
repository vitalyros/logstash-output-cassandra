# encoding: utf-8
require 'cassandra'
require 'logstash/outputs/cassandra/backoff_retry_policy'

module LogStash; module Outputs; module Cassandra
  class SafeSubmitter
    def initialize(options)
      @statement_cache = {}
      @logger = options['logger']
      setup_cassandra_session(options)
    end

    def submit(actions)
      begin
        futures = actions.map do |action|
          query = get_query(action)
          execute_async(query, action['data'].values)
        end
        futures.each(&:join)
      rescue Exception => e
        @logger.error('Failed to send batch to cassandra', :actions => actions, :exception => e, :backtrace => e.backtrace)
      end
    end

    private
    def setup_cassandra_session(options)
      cluster = options['cassandra'].cluster(
        username: options['username'],
        password: options['password'],
        protocol_version: options['protocol_version'],
        hosts: options['hosts'],
        port: options['port'],
        consistency: options['consistency'].to_sym,
        timeout: options['request_timeout'],
        retry_policy: get_retry_policy(options['retry_policy']),
        logger: options['logger']
      )
      @session = cluster.connect(options['keyspace'])
    end

    def get_retry_policy(retry_policy)
      case retry_policy['type']
        when 'default'
          return ::Cassandra::Retry::Policies::Default.new
        when 'downgrading_consistency'
          return ::Cassandra::Retry::Policies::DowngradingConsistency.new
        when 'failthrough'
          return ::Cassandra::Retry::Policies::Fallthrough.new
        when 'backoff'
          return ::Cassandra::Retry::Policies::Backoff.new({
            'backoff_type' => retry_policy['backoff_type'], 'backoff_size' => retry_policy['backoff_size'],
            'retry_limit' => retry_policy['retry_limit'], 'logger' => @logger
          })
        else
          raise ArgumentError, "unknown retry policy type: #{retry_policy['type']}"
      end
    end

    def get_query(action)
      @logger.debug('generating query for action', :action => action)
      action_data = action['data']
      query =
"INSERT INTO #{action['table']} (#{action_data.keys.join(', ')})
VALUES (#{('?' * action_data.keys.count).split(//) * ', '})"
      unless @statement_cache.has_key?(query)
        @logger.debug('preparing new query', :query => query)
        @statement_cache[query] = @session.prepare(query)
      end
      @statement_cache[query]
    end

    def execute_async(query, arguments)
      future = @session.execute_async(query, arguments: arguments)
      future.on_failure { |error|
        @logger.error('error executing insert', :query => query, :arguments => arguments, :error => error)
      }
      future.on_complete { |value, error|
        unless error.nil?
          @logger.error('error executing insert', :query => query, :arguments => arguments, :error => error)
        end
      }
      future
    end
  end
end end end
