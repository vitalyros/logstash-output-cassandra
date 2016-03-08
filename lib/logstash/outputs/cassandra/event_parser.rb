# encoding: utf-8
require "time"
require "cassandra"


module LogStash; module Outputs; module Cassandra
  class EventParser
    def initialize(opts)
      @logger = opts['logger']
      @table = opts['table']
      @filter_transform_event_key = opts['filter_transform_event_key']
      assert_filter_transform_structure(opts['filter_transform']) if opts['filter_transform']
      @filter_transform = opts['filter_transform']
      @hints = opts['hints']
      @ignore_bad_values = opts['ignore_bad_values']
    end

    def parse(event)
      action = {}
      action["table"] = event.sprintf(@table)
      filter_transform = get_filter_transform(event)
      if filter_transform
        action["data"] = {}
        for filter in filter_transform
          add_event_value_from_filter_to_action(event, filter, action)
        end
      else
        add_event_data_using_configured_hints(event, action)
      end

      return action
    end

    private
    def get_filter_transform(event)
      filter_transform = nil
      if @filter_transform_event_key
        filter_transform = event[@filter_transform_event_key]
        assert_filter_transform_structure(filter_transform)
      elsif @filter_transform
        filter_transform = @filter_transform
      end
      return filter_transform
    end

    def assert_filter_transform_structure(filter_transform)
      for item in filter_transform
        if !item.has_key?("event_key") || !item.has_key?("column_name")
          raise "item is incorrectly configured in filter_transform:\nitem => #{item}\nfilter_transform => #{filter_transform}"
        end
      end
    end

    def add_event_value_from_filter_to_action(event, filter, action)
      event_data = event[event.sprintf(filter["event_key"])]
      if filter.has_key?("cassandra_type")
        cassandra_type = event.sprintf(filter["cassandra_type"])
        event_data = convert_value_to_cassandra_type(event_data, cassandra_type)
      end
      column_name = event.sprintf(filter["column_name"])
      action["data"][column_name] = event_data
    end

    def add_event_data_using_configured_hints(event, action)
      action["data"] = event.to_hash()
      # Filter out @timestamp, @version, etc to be able to use elasticsearch input plugin directly
      action["data"].reject!{|key| %r{^@} =~ key}
      @hints.each do |event_key, cassandra_type|
        if action["data"].has_key?(event_key)
          event_data = convert_value_to_cassandra_type(action["data"][event_key], cassandra_type)
        end
      end
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
            when 'timeuuid'
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
        when 'timestamp'
          return ::Cassandra::Types::Timestamp
        when 'inet'
          return ::Cassandra::Types::Inet
        when 'float'
          return ::Cassandra::Types::Float
        when 'varchar'
          return ::Cassandra::Types::Varchar
        when 'text'
          return ::Cassandra::Types::Text
        when 'blob'
          return ::Cassandra::Types::Blob
        when 'ascii'
          return ::Cassandra::Types::Ascii
        when 'bigint'
          return ::Cassandra::Types::Bigint
        when 'counter'
          return ::Cassandra::Types::Counter
        when 'int'
          return ::Cassandra::Types::Int
        when 'varint'
          return ::Cassandra::Types::Varint
        when 'boolean'
          return ::Cassandra::Types::Boolean
        when 'decimal'
          return ::Cassandra::Types::Decimal
        when 'double'
          return ::Cassandra::Types::Double
        when 'timeuuid'
          return ::Cassandra::Types::Timeuuid
        when /^set\((.*)\)$/
          set_type = get_cassandra_type_generator($1)
          return ::Cassandra::Types::Set.new(set_type)
        else
          raise "Unknown cassandra_type #{name}"
      end
    end
  end
end end end
