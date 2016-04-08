# encoding: utf-8
# This is a version of the default retry policy (https://github.com/datastax/ruby-driver/blob/v2.1.5/lib/cassandra/retry/policies/default.rb) with backoff retry configuration options
require "cassandra"

module Cassandra
  module Retry
    module Policies
      class Backoff
        include Policy

        def initialize(opts)
          @logger = opts["logger"]
          @backoff_type = opts["backoff_type"]
          @backoff_size = opts["backoff_size"]
          @retry_limit = opts["retry_limit"]
        end

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          return retry_with_backoff({ :statement => statement, :consistency => consistency, :required => required, :received => received, :retrieved => retrieved, :retries => retries}) { |opts|
            if received >= required && !retrieved
              try_again(opts[:consistency])
            else
              try_next_host
            end
          }
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          return retry_with_backoff({ :statement => statement, :consistency => consistency, :type => type, :required => required, :received => received, :retries => retries}) { |opts|
            if opts[:received].zero?
              try_next_host
            elsif opts[:type] == :batch_log
              try_again(opts[:consistency])
            else
              reraise
            end
          }
        end

        def unavailable(statement, consistency, required, alive, retries)
          return retry_with_backoff({ :statement => statement, :consistency => consistency, :required => required, :alive => alive, :retries => retries }) { |opts|
            try_next_host
          }
        end

        def retry_with_backoff(opts)
          if opts[:retries] > @retry_limit
            @logger.error('backoff retries exhausted', :opts => opts)
            return reraise
          end

          @logger.error('activating backoff wait', :opts => opts)
          backoff_wait_before_next_retry(opts[:retries])

          return yield(opts)
        end

        def backoff_wait_before_next_retry(retries)
          backoff_wait_time = calculate_backoff_wait_time(retries)
          Kernel::sleep(backoff_wait_time)
        end

        def calculate_backoff_wait_time(retries)
          backoff_wait_time = 0
          case @backoff_type
          when "**"
            backoff_wait_time = @backoff_size ** retries
          when "*"
            backoff_wait_time = @backoff_size * retries
          end
          return backoff_wait_time
        end
      end
    end
  end
end
