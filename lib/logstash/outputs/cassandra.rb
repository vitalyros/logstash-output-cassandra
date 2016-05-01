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

  # An optional hash describing how / what to transform / filter from the original event
  # Each key is expected to be of the form { event_key => "..." column_name => "..." cassandra_type => "..." }
  # Event level processing (e.g. %{[key]}) is supported for all three
  # In case you only want to do string expansion (e.g. in the case of adding event specific dates) you can add the expansion_only key with a value of true
  # Example: using { event_key => "%{+yyyyMMddHH}" column_name => "date" expansion_only => true } will result in a date column with a string of the specified format
  config :filter_transform, :validate => :array, :default => []

  # An optional string which points to the event specific location from which to pull the filter_transform definition
  # The contents need to conform with those defined for the filter_transform config setting
  # Event level processing (e.g. %{[key]}) is supported
  config :filter_transform_event_key, :validate => :string, :default => nil

  # An optional hints hash which will be used in case filter_transform or filter_transform_event_key are not in use
  # It is used to trigger a forced type casting to the cassandra driver types in
  # the form of a hash from column name to type name in the following manner:
  # hints => {
  #    id => "int"
  #    at => "timestamp"
  #    resellerId => "int"
  #    errno => "int"
  #    duration => "float"
  #    ip => "inet" }
  config :hints, :validate => :hash, :default => {}

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
      'logger' => @logger, 'table' => @table,
      'filter_transform_event_key' => @filter_transform_event_key, 'filter_transform' => @filter_transform,
      'hints' => @hints, 'ignore_bad_values' => @ignore_bad_values
    )
  end

  def setup_safe_submitter
    @safe_submitter = ::LogStash::Outputs::Cassandra::SafeSubmitter.new(
      'logger' => @logger, 'cassandra' => ::Cassandra,
      'hosts' => @hosts, 'port' => @port, 'username' => @username, 'password' => @password,
      'consistency' => @consistency, 'request_timeout' => @request_timeout, 'retry_policy' => @retry_policy,
      'keyspace' => @keyspace
    )
  end

  def setup_buffer_and_handler
    @buffer = ::LogStash::Outputs::Cassandra::Buffer.new(@logger, @flush_size, @idle_flush_time) do |actions|
      @safe_submitter.submit(actions)
    end
  end
end
