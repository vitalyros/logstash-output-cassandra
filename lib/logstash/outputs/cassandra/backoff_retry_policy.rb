# encoding: utf-8
# This is a version of the default retry policy (https://github.com/datastax/ruby-driver/blob/v2.1.5/lib/cassandra/retry/policies/default.rb) with backoff retry configuration options
require 'cassandra'

module Cassandra
  module Retry
    module Policies
      class Backoff
        include ::Cassandra::Retry::Policy

        def initialize(opts)
          @logger = opts['logger']
          @backoff_type = opts['backoff_type']
          @backoff_size = opts['backoff_size']
          @retry_limit = opts['retry_limit']
        end

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          retry_with_backoff({ :statement => statement, :consistency => consistency, :required => required,
                               :received => received, :retrieved => retrieved, :retries => retries })
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          retry_with_backoff({ :statement => statement, :consistency => consistency, :type => type,
                               :required => required, :received => received, :retries => retries })
        end

        def unavailable(statement, consistency, required, alive, retries)
          retry_with_backoff({ :statement => statement, :consistency => consistency, :required => required,
                               :alive => alive, :retries => retries })
        end

        def retry_with_backoff(opts)
          if @retry_limit > -1 && opts[:retries] > @retry_limit
            @logger.error('backoff retries exhausted', :opts => opts)
            return reraise
          end

          @logger.error('activating backoff wait', :opts => opts)
          backoff_wait_before_next_retry(opts[:retries])

          try_again(opts[:consistency])
        end

        def backoff_wait_before_next_retry(retries)
          backoff_wait_time = calculate_backoff_wait_time(retries)
          Kernel::sleep(backoff_wait_time)
        end

        def calculate_backoff_wait_time(retries)
          case @backoff_type
            when '**'
              return @backoff_size ** retries
            when '*'
              return @backoff_size * retries
            else
              raise ArgumentError, "unknown backoff type #{@backoff_type}"
          end
        end
      end
    end
  end
end
