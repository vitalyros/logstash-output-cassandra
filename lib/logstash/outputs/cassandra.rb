# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/outputs/cassandra/buffer'
require 'logstash/outputs/cassandra/event_parser'
require 'logstash/outputs/cassandra/safe_submitter'

class LogStash::Outputs::CassandraOutput < LogStash::Outputs::Base

  milestone 1

  config_name 'cassandra'

  # List of Cassandra hostname(s) or IP-address(es)
  config :hosts, :validate => :array, :required => true

  # The port cassandra is listening to
  config :port, :validate => :number, :default => 9042, :required => true

  # The protocol version to use with cassandra
  config :protocol_version, :validate => :number, :default => 4

  # Cassandra consistency level.
  # Options: "any", "one", "two", "three", "quorum", "all", "local_quorum", "each_quorum", "serial", "local_serial", "local_one"
  # Default: "one"
  config :consistency, :validate => [ 'any', 'one', 'two', 'three', 'quorum', 'all', 'local_quorum', 'each_quorum', 'serial', 'local_serial', 'local_one' ], :default => 'one'

  # The keyspace to use
  config :keyspace, :validate => :string, :required => true

  # The table to use (event level processing (e.g. %{[key]}) is supported)
  config :table, :validate => :string, :required => true

  # Username
  config :username, :validate => :string, :required => true

  # Password
  config :password, :validate => :string, :required => true

  # Defines mapping of event data to a cassandra table.
  # It is required to set the the types of these cassandra columns.
  # Supports simple cassandra types, sets, lists and user defined types.
  # User defined types can be used only if they are defined in user_defined_types config.
  # It is possible to define the behavior in cases when event data is absent or invalid (impossible to transform to cassandra data type).
  # The config is an array of hashes with the following keys:
  # * name => mandatory string. Name of the cassandra column to save to.
  # * type => mandatory string. Cassandra type of the column.
  # * key => optional string. A pointer to saved event data. Either a top level event key, or a path to the data in [..][..].. notation.
  #     e.g. [top_level_key][child_key_1][child_key_2]
  #     if nil, a `name` parameter value will be used instead, [..][..].. notation will not be supported in this case
  # * on_nil => optional string. Default value is "fail".
  #     Defines behavior in the case when event data is absent (or nil) by the given event_key
  # * on_invalid => optional string. Default value is "fail".
  #     Defines behavior in the case when the data by given event_key can't be translated to the given cassandra_type
  #
  #     Both on_nil and on_invalid have the same form. Its one of the predefined strings:
  #     "fail" => an exception will be raised, consequently the event will not be saved to cassandra table.
  #     "ignore" => this column will either be skipped entirely (absent in an insert request to cassandra).
  #       This behavior can be used for optional columns, when it is unnecessary for incoming events to contain full set of data.
  #       This behavior can't be used for partitioning or clustering keys of the cassandra table, since they are mandatory.
  #     "ignore warn" => same behavior, as for "ignore", but a detailed warning message will be logged
  #     "default" => the value, set in the "default" field will be used.
  #       If the "default" field is not set, the default value will be chosen automatically, depending on the cassandra_type.
  #     "default warn" => same behavior, as for "default", but a detailed warning message will be logged.
  #
  # * default => optional string. Undefined by default. Default value used when either on_nil, or on_invalid is set to 'default'
  # Examples:
  #   for optional column { key => "[data][comment]" name => "comment" type => "text" on_nil => "ignore" on_invalid => "ignore warn"}
  #   for default value column { key => "[data][amount]" name => "amount" type => "int" on_nil => "default" default => "0"}
  #   for mandatory column { name => "id" type => "text" }
  #   for user defined type column, see `user_defined_types` example { name => "user_data" type => "user_data" }
  config :columns, :validate => :array, :default => []


  # Defines types that can be used in `columns` configuration
  # These types are intended to map a hash field that is part of an event to the value of a cassandra user defined type.
  # It is possible to define the behavior in cases when event data is absent or invalid (impossible to transform to cassandra data type)
  # NOTE: in runtime, if after the transformation and application of on_nil / on_invalid behavior the resulting user defined type value is an empty hash - will raise an error.
  # The config structure:
  # * name => mandatory string. Name of the type as it can be used in `columns` configuration. May not correspond to the name of the cassandra user defined type.
  # * fields => an array of hashes, mandatory, at least one item is required.
  #   Can contain following key => value pairs:
  #   * name => mandatory string. Name of the cassandra column to save to.
  #   * type => mandatory string. Cassandra type of the column.
  #   * key => optional string. A pointer to saved event data. Either a top level event key, or a path to the data in [..][..].. notation.
  #       e.g. [top_level_key][child_key_1][child_key_2]
  #       if nil, a `name` parameter value will be used instead, [..][..].. notation will not be supported in this case
  # * on_nil => optional string,
  #     Defines behavior in the case when event data is absent (or nil) by the given event_key
  # * on_invalid => optional string. Default value is "fail".
  #     Defines behavior in the case when the data by given event_key can't be translated to the given cassandra_type
  #
  #     See columns config for possible values of on_nil and on_invalid
  #
  # Examples:
  #   user_defined_types => [
  #     { name => "user_data"
  #       fields => [
  #         { key => "name" type => "text" },
  #         { key => "age" type => "int" },
  #         { key => "city" type => "text" on_nil => "ignore" } # optional field
  #       ]
  #     }
  #   ]
  # #
  config :user_defined_types, :validate => :array, :default => []

  # The retry policy to use (the default is the default retry policy)
  # the hash requires the name of the policy and the params it requires
  # The available policy names are:
  # * default => retry once if needed / possible
  # * downgrading_consistency => retry once with a best guess lowered consistency
  # * failthrough => fail immediately (i.e. no retries)
  # * backoff => a version of the default retry policy but with configurable backoff retries
  # The backoff options are as follows:
  # * backoff_type => either * or ** for linear and exponential backoffs respectively
  # * backoff_size => the left operand for the backoff type in seconds
  # * retry_limit => the maximum amount of retries to allow per query
  # example:
  # using { "type" => "backoff" "backoff_type" => "**" "backoff_size" => 2 "retry_limit" => 10 } will perform 10 retries with the following wait times: 1, 2, 4, 8, 16, ... 1024
  # NOTE: there is an underlying assumption that the insert query is idempotent !!!
  # NOTE: when the backoff retry policy is used, it will also be used to handle pure client timeouts and not just ones coming from the coordinator
  config :retry_policy, :validate => :hash, :default => { 'type' => 'default' }, :required => true

  # The command execution timeout
  config :request_timeout, :validate => :number, :default => 1

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

  # Cassandra record ttl in seconds
  # If absent, ttl is not used for the record
  config :ttl, :validate => :number, :default => nil


  def register
    setup_event_parser
    setup_safe_submitter
    setup_buffer_and_handler
  end

  def receive(event)
    @buffer << @event_parser.parse(event)
  end

  def multi_receive(events)
    events.each_slice(@flush_size) do |slice|
      @safe_submitter.submit(slice.map {|event| @event_parser.parse(event) })
    end
  end

  def teardown
    close
  end

  def close
    @buffer.stop
  end

  def flush
    @buffer.flush
  end

  private
  def setup_event_parser
    @event_parser = ::LogStash::Outputs::Cassandra::EventParser.new(
      'logger' => @logger, 'table' => @table, 'columns' => @columns, 'user_defined_types' => @user_defined_types
    )
  end

  def setup_safe_submitter
    @safe_submitter = ::LogStash::Outputs::Cassandra::SafeSubmitter.new(
      'logger' => @logger, 'cassandra' => ::Cassandra,
      'hosts' => @hosts, 'port' => @port, 'username' => @username, 'password' => @password,
      'consistency' => @consistency, 'request_timeout' => @request_timeout, 'retry_policy' => @retry_policy,
      'keyspace' => @keyspace, 'ttl' => @ttl
    )
  end

  def setup_buffer_and_handler
    @buffer = ::LogStash::Outputs::Cassandra::Buffer.new(@logger, @flush_size, @idle_flush_time) do |actions|
      @safe_submitter.submit(actions)
    end
  end
end
