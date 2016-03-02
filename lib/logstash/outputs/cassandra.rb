# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "time"
require "logstash/outputs/cassandra/buffer"
require "cassandra"


class LogStash::Outputs::Cassandra < LogStash::Outputs::Base

  milestone 1

  config_name "cassandra"

  # List of Cassandra hostname(s) or IP-address(es)
  config :hosts, :validate => :array, :required => true

  # Cassandra consistency level.
  # Options: "any", "one", "two", "three", "quorum", "all", "local_quorum", "each_quorum", "serial", "local_serial", "local_one"
  # Default: "one"
  config :consistency, :validate => [ "any", "one", "two", "three", "quorum", "all", "local_quorum", "each_quorum", "serial", "local_serial", "local_one" ], :default => "one"

  # The keyspace to use
  config :keyspace, :validate => :string, :required => true

  # The table to use (event level processing (e.g. %{[key]}) is supported)
  config :table, :validate => :string, :required => true

  # Username
  config :username, :validate => :string, :required => true

  # Password
  config :password, :validate => :string, :required => true

  # An optional hash describing how / what to transform / filter from the original event
  # Each key is expected to be of the form { event_data => "..." column_name => "..." cassandra_type => "..." }
  # Event level processing (e.g. %{[key]}) is supported for all three
  config :filter_transform, :validate => :array, :default => nil

  # An optional string which points to the event specific location from which to pull the filter_transform definition
  # The contents need to conform with those defined for the filter_transform config setting
  # Event level processing (e.g. %{[key]}) is supported
  config :filter_transform_event_key, :validate => :string, :default => nil

  # The retry policy to use
  # The available options are:
  # * default => retry once if needed / possible
  # * downgrading_consistency => retry once with a best guess lowered consistency
  # * failthrough => fail immediately (i.e. no retries)
  config :retry_policy, :validate => [ "default", "downgrading_consistency", "failthrough" ], :default => "default", :required => true

  # The command execution timeout
  config :request_timeout, :validate => :number, :default => 5

  # Ignore bad values
  config :ignore_bad_values, :validate => :boolean, :default => false

  # In Logstashes >= 2.2 this setting defines the maximum sized bulk request Logstash will make
  # You you may want to increase this to be in line with your pipeline's batch size.
  # If you specify a number larger than the batch size of your pipeline it will have no effect,
  # save for the case where a filter increases the size of an inflight batch by outputting
  # events.
  #
  # In Logstashes <= 2.1 this plugin uses its own internal buffer of events.
  # This config option sets that size. In these older logstashes this size may
  # have a significant impact on heap usage, whereas in 2.2+ it will never increase it.
  # To make efficient bulk API calls, we will buffer a certain number of
  # events before flushing that out to Cassandra. This setting
  # controls how many events will be buffered before sending a batch
  # of events. Increasing the `flush_size` has an effect on Logstash's heap size.
  # Remember to also increase the heap size using `LS_HEAP_SIZE` if you are sending big commands
  # or have increased the `flush_size` to a higher value.
  config :flush_size, :validate => :number, :default => 500

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # Logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  def register()
    @statement_cache = {}
    assert_filter_transform_structure(@filter_transform) if @filter_transform
    setup_buffer_and_handler()
    assert_filter_transform_structure()
  end

  def receive(event)
    @buffer << build_cassandra_action(event)
  end

  # Receive an array of events and immediately attempt to index them (no buffering)
  def multi_receive(events)
    events.each_slice(@flush_size) do |slice|
      safe_submit(slice.map {|e| build_cassandra_action(e) })
    end
  end

  def teardown()
    close()
  end

  def close()
    @buffer.stop()
  end

  private
  def assert_filter_transform_structure(filter_transform)
    for item in filter_transform
      if !item.has_key?("event_key") || !item.has_key?("column_name") || !item.has_key?("cassandra_type")
        raise "item is incorrectly configured in filter_transform:\nitem => #{item}\nfilter_transform => #{filter_transform}"
      end
    end
  end

  def setup_buffer_and_handler
    @buffer = ::LogStash::Outputs::Cassandra::Buffer.new(@logger, @flush_size, @idle_flush_time) do |actions|
      safe_submit(actions)
    end
  end

  def setup_cassandra_session()
    cluster = Cassandra.cluster(
      username: @username,
      password: @password,
      hosts: @hosts,
      consistency: @consistency.to_sym,
      timeout: @request_timeout,
      retry_policy: get_retry_policy(@retry_policy),
      logger: @logger
    )
    @session = cluster.connect(@keyspace)
    @logger.info("New cassandra session created",
                 :username => @username, :hosts => @hosts, :keyspace => @keyspace)
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

  def build_cassandra_action(event)
    action = {}
    action["table"] = event.sprintf(@table)
    filter_transform = get_field_transform(event)
    if filter_transform
      action["data"] = {}
      for filter in filter_transform
        add_event_value_from_filter_to_action(event, filter, action)
      end
    else
      action["data"] = event.to_hash()
      # Filter out @timestamp, @version, etc to be able to use elasticsearch input plugin directly
      action["data"].reject!{|key| %r{^@} =~ key}
      # TODO: add the hint thing here?!...
      #convert_values_to_cassandra_types!(action)
    end

    return action
  end

  def get_field_transform(event)
    filter_transform = nil
    if @filter_transform_event_key
      filter_transform = event.sprintf(@filter_transform_event_key)
      assert_filter_transform_structure(filter_transform)
    elsif @filter_transform
      filter_transform = @filter_transform
    end
    return filter_transform
  end

  def add_event_value_from_filter_to_action(event, filter, action)
    event_data = event.sprintf(filter["event_data"])
    if filter.has_key?("cassandra_type")
      cassandra_type = event.sprintf(filter["cassandra_type"])
      event_data = convert_value_to_cassandra_type(event_data, cassandra_type)
    end
    column_name = event.sprintf(filter["column_name"])
    action["data"][column_name] = event_data
  end

  def safe_submit(actions)
    begin
      batch = prepare_batch(actions)
      @session.execute(batch)
      @logger.info("Batch sent successfully")
    rescue Exception => e
      @logger.warn("Failed to send batch (error: #{e.to_s}).")
    end
  end

  def prepare_batch(actions)
    statement_and_values = []
    for action in actions
      query = "INSERT INTO #{@keyspace}.#{action["table"]} (#{action["data"].keys.join(', ')})
        VALUES (#{("?" * action["data"].keys.count).split(//) * ", "})"

      if !@statement_cache.key?(query)
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

  def convert_value_to_cassandra_type(event_data, cassandra_type)
    generator = get_cassandra_type_generator(cassandra_type)
    typed_event_data = nil
    begin
      typed_event_data = generator.new(event_data)
    rescue Exception => e
      error_message = "Cannot convert `value (`#{event_data}`) to `#{cassandra_type}` type"
      if @ignore_bad_values
        case event_data
          when 'int', 'varint', 'bigint', 'double', 'decimal', 'counter'
            typed_event_data = 0
          when 'uuid', 'timeuuid'
            typed_event_data = generator.new("00000000-0000-0000-0000-000000000000")
          when 'timestamp'
            typed_event_data = generator.new(Time::parse("1970-01-01 00:00:00"))
          when 'inet'
            typed_event_data = generator.new("0.0.0.0")
          when 'float'
            typed_event_data = generator.new(0)
          when 'boolean'
            typed_event_data = generator.new(false)
          when 'text', 'varchar', 'ascii'
            typed_event_data = generator.new(0)
          when 'blob'
            typed_event_data = generator.new(nil)
          when /^set\((.*)\)$/
            typed_event_data = generator.new([])
        end
        @logger.warn(error_message, :exception => e, :backtrace => e.backtrace)
      else
        @logger.error(error_message, :exception => e, :backtrace => e.backtrace)
        raise error_message
      end
    end
    return typed_event_data
  end

  def get_cassandra_type_generator(name)
    case name
      when 'uuid'
        return Cassandra::Types::Uuid
      when 'timestamp'
        return Cassandra::Types::Timestamp
      when 'inet'
        return Cassandra::Types::Inet
      when 'float'
        return Cassandra::Types::Float
      when 'varchar'
        return Cassandra::Types::Varchar
      when 'text'
        return Cassandra::Types::Text
      when 'blob'
        return Cassandra::Types::Blog
      when 'ascii'
        return Cassandra::Types::Ascii
      when 'bigint'
        return Cassandra::Types::Bigint
      when 'counter'
        return Cassandra::Types::Counter
      when 'int'
        return Cassandra::Types::Int
      when 'varint'
        return Cassandra::Types::Varint
      when 'boolean'
        return Cassandra::Types::Boolean
      when 'decimal'
        return Cassandra::Types::Decimal
      when 'double'
        return Cassandra::Types::Double
      when 'timeuuid'
        return Cassandra::Types::Timeuuid
      when /^set\((.*)\)$/
        set_type = get_cassandra_type_generator($1)
        return Cassandra::Types::Set(set_type)
      else
        raise "Unknown cassandra_type #{cassandra_type}"
    end
  end

  # def convert_values_to_cassandra_types!(msg)
  #   @hints.each do |key, value|
  #     if msg.key?(key)
  #
  #     end
  #   end
  # end
end
