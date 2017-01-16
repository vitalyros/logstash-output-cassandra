# encoding: utf-8
require 'time'
require 'cassandra'

module LogStash; module Outputs; module Cassandra
  # Responsible for accepting events from the pipeline and returning actions for the SafeSubmitter
  class EventParser
    def initialize(options)
      @logger = options['logger']
      @table = options['table']
      @filter_transform_event_key = options['filter_transform_event_key']
      assert_filter_transform_structure(options['filter_transform']) if options['filter_transform']
      @filter_transform = options['filter_transform']
      @columns = options['columns']
      @hints = options['hints']
      @ignore_bad_values = options['ignore_bad_values']
      if @columns
        @column_paths = {}
        @columns.each do |column|
          path = column['event_key'].split(/[\[\]]/).map { |s| s.strip }.select { |s| !s.empty? }
          if path.empty?
            raise ArgumentError, "Invalid configuration. Invalid event_key value `#{column['event_key']}` in column config `#{column}`"
          else
            @column_paths[column] = path
          end
        end
      end
    end

    def parse(event)
      action = {}
      begin
        action['table'] = event.sprintf(@table)
        filter_transform = get_filter_transform(event)
        if filter_transform
          action['data'] = {}
          filter_transform.each { |filter|
            add_event_value_from_filter_to_action(event, filter, action)
          }
        elsif @columns
          action['data'] = {}
          @columns.each { |column|
            add_event_value_from_column_definition(event, column, action)
          }
        else
          add_event_data_using_configured_hints(event, action)
        end
        @logger.debug('event parsed to action', :action => action)
      rescue Exception => e
        @logger.error('failed parsing event', :event => event, :error => e)
        action = nil
      end
      action
    end

    private
    def get_filter_transform(event)
      filter_transform = nil
      if @filter_transform_event_key
        filter_transform = event[@filter_transform_event_key]
        assert_filter_transform_structure(filter_transform)
      elsif @filter_transform.length > 0
        filter_transform = @filter_transform
      end
      filter_transform
    end

    def assert_filter_transform_structure(filter_transform)
      filter_transform.each { |item|
        if !item.has_key?('event_key') || !item.has_key?('column_name')
          raise ArgumentError, "item is incorrectly configured in filter_transform:\nitem => #{item}\nfilter_transform => #{filter_transform}"
        end
      }
    end

    def add_event_value_from_column_definition(event, column, action)
      raw_event_data = true
      event_data = event
      column_path = @column_paths[column]
      for key in column_path
        event_data = event_data[key]
        if event_data == nil
          case column['on_nil']
            when nil, 'fail'
              raise ArgumentError, "Event data is nil by key `#{column['event_key']}`. event: #{event}"
            when 'ignore'
              return
            when 'ignore warn'
              @logger.warn("Event data is nil by key `#{column['event_key']}`. event: #{event}")
              return
            when 'default'
              event_data = get_default_value(event, column)
              raw_event_data = false
            when 'default warn'
              @logger.warn("Event data is nil by key `#{column['event_key']}`. event: #{event}")
              event_data = get_default_value(event, column)
              raw_event_data = false
            else
              raise ArgumentError, "Invalid configuration. Invalid on_nil value `#{column['on_nil']}` in column config `#{column}`"
          end
        end
      end
      if raw_event_data
        cassandra_type = event.sprintf(column['cassandra_type'])
        begin
          event_data = convert_value_to_cassandra_type(event_data, cassandra_type)
        rescue Exception => e
          case column['on_invalid']
            when nil, 'fail'
              raise ArgumentError, "Event data by key `#{column['event_key']}` is invalid for type `#{cassandra_type}`. event: #{event}"
            when 'ignore'
              return
            when 'ignore warn'
              @logger.warn("Event data by key `#{column['event_key']}` is invalid for type `#{cassandra_type}`. event: #{event}")
              return
            when 'default'
              event_data = get_default_value(event, column)
            when 'default warn'
              @logger.warn("Event data by key `#{column['event_key']}` is invalid for type `#{cassandra_type}`. event: #{event}")
              event_data = get_default_value(event, column)
            else
              raise ArgumentError, "Invalid configuration. Invalid on_nil value `#{column['on_nil']}` in column config `#{column}`"
          end
        end
      end
      column_name = event.sprintf(column['column_name'])
      action['data'][column_name] = event_data
    end

    def add_event_value_from_filter_to_action(event, filter, action)
      event_data = event.sprintf(filter['event_key'])
      unless filter.fetch('expansion_only', false)
        event_data = event[event_data]
      end
      if filter.has_key?('cassandra_type')
        cassandra_type = event.sprintf(filter['cassandra_type'])
        event_data = convert_value_to_cassandra_type_or_default_if_configured(event_data, cassandra_type)
      end
      column_name = event.sprintf(filter['column_name'])
      action['data'][column_name] = event_data
    end

    def add_event_data_using_configured_hints(event, action)
      action_data = event.to_hash.reject { |key| %r{^@} =~ key }
      
      @hints.each do |event_key, cassandra_type|
        if action_data.has_key?(event_key)
          action_data[event_key] = convert_value_to_cassandra_type_or_default_if_configured(action_data[event_key], cassandra_type)
        end
      end
      action['data'] = action_data
    end

    def convert_value_to_cassandra_type_or_default_if_configured(event_data, cassandra_type)
      typed_event_data = nil
      begin
        typed_event_data = convert_value_to_cassandra_type(event_data, cassandra_type)
      rescue Exception => e
        error_message = "Cannot convert `value (`#{event_data}`) to `#{cassandra_type}` type"
        if @ignore_bad_values
          typed_event_data = get_default_value_by_type(cassandra_type)
          @logger.warn(error_message, :exception => e, :backtrace => e.backtrace)
        else
          @logger.error(error_message, :exception => e, :backtrace => e.backtrace)
          raise error_message
        end
      end
      typed_event_data
    end

    def get_default_value(event, column)
      cassandra_type = event.sprintf(column['cassandra_type'])
      default = event.sprintf(column['default'])
      if default == nil
        return get_default_value_by_type(cassandra_type)
      else
        return convert_value_to_cassandra_type(default, cassandra_type)
      end
    end

    def get_default_value_by_type(cassandra_type)
      case cassandra_type
        when 'float', 'int', 'varint', 'bigint', 'double', 'counter', 'timestamp'
          return convert_value_to_cassandra_type(0, cassandra_type)
        when 'timeuuid'
          return convert_value_to_cassandra_type('00000000-0000-0000-0000-000000000000', cassandra_type)
        when 'inet'
          return convert_value_to_cassandra_type('0.0.0.0', cassandra_type)
        when /^list<.*>$/
          return convert_value_to_cassandra_type([], cassandra_type)
        when /^set<.*>$/
          return convert_value_to_cassandra_type([], cassandra_type)
        else
          raise ArgumentError, "unable to provide a default value for type #{event_data}"
      end
    end

    def convert_value_to_cassandra_type(event_data, cassandra_type)
      case cassandra_type
        when 'timestamp'
          converted_value = event_data
          if converted_value.is_a?(Numeric)
            converted_value = Time.at(converted_value)
          elsif converted_value.respond_to?(:to_s)
            converted_value = Time::parse(event_data.to_s)
          end
          return ::Cassandra::Types::Timestamp.new(converted_value)
        when 'inet'
          return ::Cassandra::Types::Inet.new(event_data)
        when 'float'
          return ::Cassandra::Types::Float.new(event_data)
        when 'varchar'
          return ::Cassandra::Types::Varchar.new(event_data)
        when 'text'
          return ::Cassandra::Types::Text.new(event_data)
        when 'blob'
          return ::Cassandra::Types::Blob.new(event_data)
        when 'ascii'
          return ::Cassandra::Types::Ascii.new(event_data)
        when 'bigint'
          return ::Cassandra::Types::Bigint.new(event_data)
        when 'counter'
          return ::Cassandra::Types::Counter.new(event_data)
        when 'int'
          return ::Cassandra::Types::Int.new(event_data)
        when 'varint'
          return ::Cassandra::Types::Varint.new(event_data)
        when 'boolean'
          return ::Cassandra::Types::Boolean.new(event_data)
        when 'decimal'
          return ::Cassandra::Types::Decimal.new(event_data)
        when 'double'
          return ::Cassandra::Types::Double.new(event_data)
        when 'timeuuid'
          return ::Cassandra::Types::Timeuuid.new(event_data)
        when /^list<(.*)>$/
          # convert each value
          # then add all to an array
          converted_items = []
          set_type = $1
          @logger.warn("LIST #{event_data} of type (#{set_type})")
          event_data.each { |item|
            @logger.warn("CONVERT ITEM (#{item}) of type (#{set_type})")
            converted_item = convert_value_to_cassandra_type(item, set_type)
            converted_items << converted_item
          }
          return converted_items
        when /^set<(.*)>$/
          # convert each value
          # then add all to an array and convert to set
          converted_items = ::Set.new
          set_type = $1
          event_data.each { |item|
            converted_item = convert_value_to_cassandra_type(item, set_type)
            converted_items.add(converted_item)
          }
          return converted_items
        else
          raise "Unknown cassandra_type #{name}"
      end
    end
  end
end end end
