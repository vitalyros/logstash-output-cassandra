# encoding: utf-8
require "cassandra"


module LogStash; module Outputs; module Cassandra
  class SafeSubmitter
    def initialize(logger, username, password, hosts, consistency, request_timeout, retry_policy, keyspace)
      @statement_cache = {}
      @logger = logger
      @keyspace = keyspace
      setup_cassandra_session(logger, username, password, hosts, consistency, request_timeout, retry_policy)
    end

    def submit(actions)
      begin
        batch = prepare_batch(actions)
        @session.execute(batch)
        @logger.info("Batch sent successfully")
      rescue Exception => e
        @logger.warn("Failed to send batch (error: #{e.to_s}).")
      end
    end

    private
    def setup_cassandra_session(logger, username, password, hosts, consistency, request_timeout, retry_policy)
      cluster = ::Cassandra.cluster(
        username: username,
        password: password,
        hosts: hosts,
        consistency: consistency.to_sym,
        timeout: request_timeout,
        retry_policy: get_retry_policy(retry_policy),
        logger: logger
      )
      @session = cluster.connect(@keyspace)
      @logger.info("New cassandra session created",
                   :username => username, :hosts => hosts, :keyspace => @keyspace)
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
        query = "INSERT INTO #{@keyspace}.#{action["table"]} (#{action["data"].keys.join(', ')})
          VALUES (#{("?" * action["data"].keys.count).split(//) * ", "})"

        if !@statement_cache.has_key?(query)
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
