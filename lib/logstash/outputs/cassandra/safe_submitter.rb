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
        batch = prepare_batch(actions)
        @session.execute(batch)
      rescue Exception => e
        @logger.error("Failed to send batch to cassandra", :exception => e, :backtrace => e.backtrace)
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

    def prepare_batch(actions)
      statement_and_values = []
      for action in actions
        @logger.debug("generating query for action", :action => action)
        query =
"INSERT INTO #{action["table"]} (#{action["data"].keys.join(', ')})
VALUES (#{("?" * action["data"].keys.count).split(//) * ", "})"

        if !@statement_cache.has_key?(query)
          @logger.debug("new query generated", :query => query)
          @statement_cache[query] = @session.prepare(query)
        end
        statement_and_values << [@statement_cache[query], action["data"].values]
      end

      batch = @session.batch do |b|
        statement_and_values.each do |v|
          b.add(v[0], v[1])
        end
      end
      return batch
    end
  end
end end end
