# encoding: utf-8
require "cassandra"

module LogStash; module Outputs; module Cassandra
  class SafeSubmitter
    def initialize(options)
      @statement_cache = {}
      @logger = options["logger"]
      setup_cassandra_session(options)
    end

    def submit(actions)
      begin
        futures = actions.map do |action|
          query = get_query(action)
          execute_async(query, action["data"].values)
        end
        futures.each(&:join)
      rescue Exception => e
        @logger.error("Failed to send batch to cassandra", :actions => actions, :exception => e, :backtrace => e.backtrace)
      end
    end

    private
    def setup_cassandra_session(options)
      cluster = options["cassandra"].cluster(
        username: options["username"],
        password: options["password"],
        hosts: options["hosts"],
        port: options["port"],
        consistency: options["consistency"].to_sym,
        timeout: options["request_timeout"],
        retry_policy: get_retry_policy(options["retry_policy"]),
        logger: options["logger"]
      )
      @session = cluster.connect(options["keyspace"])
    end

    def get_retry_policy(policy_name)
      case policy_name
        when "default"
          return ::Cassandra::Retry::Policies::Default.new
        when "downgrading_consistency"
          return ::Cassandra::Retry::Policies::DowngradingConsistency.new
        when "failthrough"
          return ::Cassandra::Retry::Policies::Fallthrough.new
      end
    end

    def get_query(action)
      @logger.debug("generating query for action", :action => action)
      query =
"INSERT INTO #{action["table"]} (#{action["data"].keys.join(', ')})
VALUES (#{("?" * action["data"].keys.count).split(//) * ", "})"
      if !@statement_cache.has_key?(query)
        @logger.debug("new query generated", :query => query)
        @statement_cache[query] = @session.prepare(query)
      end
      return @statement_cache[query]
    end

    def execute_async(query, arguments)
      future = @session.execute_async(query, arguments: arguments)
      future.on_failure { |error|
        @logger.error("error executing insert", :query => query, :arguments => arguments, :error => error)
      }
      return future
    end
  end
end end end
