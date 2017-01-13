# encoding: utf-8
require "concurrent"
java_import java.util.ArrayList
java_import java.util.concurrent.LinkedBlockingDeque


module LogStash; module Outputs; module Cassandra
  class Buffer
    def initialize(logger, max_size, flush_interval, &block)
      @logger = logger

      @flush_lock = Mutex.new
      @stop_lock = Mutex.new

      @stopping = Concurrent::AtomicBoolean.new(false)
      @max_size = max_size
      @submit_proc = block

      @storage = LinkedBlockingDeque.new

      @last_flush = Concurrent::AtomicReference.new(Time.now)
      @flush_interval = flush_interval
      @flush_thread = spawn_interval_flusher
    end

    def spawn_interval_flusher
      Thread.new do
        loop do
          sleep 0.2
          break if stopping?
          flush_by_interval
        end
      end
    end

    def push(item)
      @storage.add(item)
      flush_if_full
    end
    alias_method :<<, :push


    # Push multiple items onto the buffer in a single operation
    def push_multi(items)
      raise ArgumentError, "push multi takes an array!, not an #{items.class}!" unless items.is_a?(Array)
      items.each {|item| push(item) }
    end

    # Handles gentle shutdown
    def stop(do_flush=true,wait_complete=true)
      @stop_lock.synchronize {
        return if stopping?
        @stopping.make_true
      }
      return if !do_flush && !wait_complete
      flush if do_flush
      @flush_thread.join if wait_complete
    end

    def contents
      java_array_to_ruby_array(@storage.toArray)
    end

    # Flushes the buffer if it has reached @max_size
    # The buffer's contents are moved into submissions of size @max_size, which are then submitted
    # Not more than one submission is generally expected to be generated at a time, but with small values of @max_size it may be
    #   possible to generate more due to the racing between threads, so flushing is done in a cycle.
    # This flushing operation doesn't make submissions smaller than @max_size, so some content may be left in the buffer.
    def flush_if_full
      submissions = []
      if @storage.length >= @max_size
        @flush_lock.synchronize {
          while @storage.length > 0 && @storage.length >= @max_size
            submissions << flush_buffer_unsafe(@max_size)
          end
        }
        submit(submissions)
      end
    end

    # Flushes the buffer if it has not been flushed for the @flush_interval
    # The buffer's contents are moved into submissions of size @max_size or smaller, which are then submitted
    # Not more than one submission is generally expected to be generated at a time, but with small values of @max_size it may be
    #   possible to generate more due to the racing between threads, so flushing is done in a cycle.
    # In contrast with the flush_if_full function, flushing done by this function always leaves the buffer empty.
    def flush_by_interval
      begin
        if last_flush_seconds_ago >= @flush_interval
          submissions = []
          @flush_lock.synchronize {
            if last_flush_seconds_ago >= @flush_interval
              @logger.debug? && @logger.debug("Flushing buffer at interval",
                                              :instance => self.inspect,
                                              :interval => @flush_interval)
              while @storage.length > 0
                submissions << flush_buffer_unsafe(@max_size)
              end
            end
          }
          submit(submissions)
        end
      rescue StandardError => e
        @logger.error("Error flushing buffer at interval!",
                     :instance => self.inspect,
                     :message => e.message,
                     :class => e.class.name,
                     :backtrace => e.backtrace
        )
      rescue Exception => e
        @logger.error("Exception flushing buffer at interval!", :error => e.message, :class => e.class.name)
      end
    end

    # Fully flushes the buffer. Flushing leaves the buffer empty
    # The buffer's contents are moved into submissions of size @max_size or smaller, which are then submitted
    def flush
      submissions = []
      @flush_lock.synchronize {
        while @storage.length > 0
          submissions << flush_buffer_unsafe(@max_size)
        end
      }
      submit(submissions)
    end

    def submit(submissions)
      submissions.each do |submission|
        # The java lists are transformed into juby arrays before submission
        if submission.size > 0
          transformed_submission = java_list_to_ruby_array submission
          begin
            @submit_proc.call(transformed_submission)
          rescue Exception => e
            @logger.error("Buffer submition failed", :error => e.message, :class => e.class.name)
            @storage.addAll(submission)
          end
        end
      end
    end

    # Flushes buffer into a submission. The submission is implemented as java list
    # Not thread safe. Should be synchronized by a flush_lock
    def flush_buffer_unsafe(submission_limit)
      submission = ArrayList.new
      @storage.drain_to(submission, submission_limit)
      @last_flush.set Time.now # This must always be set to ensure correct timer behavior
      submission
    end

    # Makes a copy of java list as simple ruby array
    def java_list_to_ruby_array java_list
      ruby_array = []
      if java_list.length > 0
        for i in (0..java_list.length - 1)
          ruby_array << java_list.get(i)
        end
      end
      ruby_array
    end

    # Makes a copy of java array as simple ruby array
    def java_array_to_ruby_array java_array
      ruby_array = []
      if java_array.length > 0
        for i in (0..java_array.length - 1)
          ruby_array << java_array[i]
        end
      end
      ruby_array
    end

    def last_flush_seconds_ago
      Time.now - @last_flush.get
    end

    def stopping?
      @stopping.true?
    end
  end
end end end
