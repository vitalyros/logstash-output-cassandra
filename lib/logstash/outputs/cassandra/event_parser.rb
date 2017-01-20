# encoding: utf-8
require 'time'
require 'cassandra'

module LogStash module Outputs module Cassandra
  # Responsible for accepting events from the pipeline and returning actions for the SafeSubmitter
  class EventParser
    def initialize(options)
      @logger  = options['logger']
      @table   = options['table']
      @columns = options['columns']
      init_simple_type_transformations
      init_udt_conf(options['user_defined_types'])
      init_column_transformations
    end

    # Prepares transformations for simple cassandra types
    def init_simple_type_transformations()
      @simple_type_transformations = {
        'timestamp' => lambda { |data, event|
          converted_value = data
          if converted_value.is_a?(Numeric)
            converted_value = Time.at(converted_value)
          elsif converted_value.respond_to?(:to_s)
            converted_value = Time::parse(data.to_s)
          end
          return ::Cassandra::Types::Timestamp.new(converted_value)
        },
        'inet'      => lambda { |data, event| return ::Cassandra::Types::Inet.new(data) },
        'float'     => lambda { |data, event| return ::Cassandra::Types::Float.new(data) },
        'varchar'   => lambda { |data, event| return ::Cassandra::Types::Varchar.new(data) },
        'text'      => lambda { |data, event| return ::Cassandra::Types::Text.new(data) },
        'blob'      => lambda { |data, event| return ::Cassandra::Types::Blob.new(data) },
        'ascii'     => lambda { |data, event| return ::Cassandra::Types::Ascii.new(data) },
        'bigint'    => lambda { |data, event| return ::Cassandra::Types::Bigint.new(data) },
        'counter'   => lambda { |data, event| return ::Cassandra::Types::Counter.new(data) },
        'int'       => lambda { |data, event| return ::Cassandra::Types::Int.new(data) },
        'varint'    => lambda { |data, event| return ::Cassandra::Types::Varint.new(data) },
        'boolean'   => lambda { |data, event| return ::Cassandra::Types::Boolean.new(data) },
        'decimal'   => lambda { |data, event| return ::Cassandra::Types::Decimal.new(data) },
        'double'    => lambda { |data, event| return ::Cassandra::Types::Double.new(data) },
        'timeuuid'  => lambda { |data, event| return ::Cassandra::Types::Timeuuid.new(data) }
      }
    end

    # Maps user defined types configurations by name
    def init_udt_conf(udt_configs_list)
      @udt_configs = {}
      udt_configs_list.each do |udt_config|
        @udt_configs[udt_config['name']] = udt_config
      end
    end

    # Prepares transformation for all columns, mentioned in columns config
    def init_column_transformations
      @columns.each do |column|
        column['transformation'] = create_column_transformation(column)
      end
    end

    def parse(event)
      action = {}
      begin
        action['table'] = event.sprintf(@table)
        action['data']  = {}
        @columns.each { |column|
          action['data'][column['name']] = column['transformation'][event, event]
        }
        @logger.debug('event parsed to action', :action => action)
      rescue Exception => e
        @logger.error('failed parsing event', :event => event, :error => e, :backtrace => e.backtrace)
        action = nil
      end
      action
    end

    def create_udt_transformation(udt_conf, full_key_path)
      udt_transformations = {}
      udt_conf['fields'].each do |field_conf|
        udt_transformations[field_conf['key']] = create_udt_field_transformation(field_conf, full_key_path)
      end
      lambda do |data, event|
        # apply transformation to every field
        result = {}
        udt_transformations.each do |key, transformation|
          result[key] = transformation[data, event]
        end
        return ::Cassandra::UDT.new(result)
      end
    end

    def create_udt_field_transformation(field_conf, full_key_path)
      key_path, raw_path = split_key_path(field_conf)
      create_field_transformation(key_path, "#{full_key_path}#{raw_path}", field_conf)
    end

    def create_column_transformation(field_conf)
      key_path, raw_path = split_key_path(field_conf)
      create_field_transformation(key_path, raw_path, field_conf)
    end

    def split_key_path(field_conf)
      raw_path = field_conf['key']
      if raw_path == nil
        raw_path = field_conf['name']
        key_path = raw_path
      else
        key_path = raw_path.split(/[\[\]]/).map { |s| s.strip }.select { |s| !s.empty? }
        if key_path.empty?
          raise ArgumentError, "Invalid configuration. Invalid key value `#{raw_path}` in config `#{field_conf}`"
        end
      end
      unless raw_path.match(/\[.*\]/)
        raw_path = "[#{raw_path}]"
      end
      return key_path, raw_path
    end

    def create_field_transformation(key_path, raw_path, field_conf)
      type = field_conf['type']
      type_transformation = create_type_transformation(type, raw_path)
      lambda do |data, event|
        needs_transformation = true
        for key in key_path
          data = data[key]
          if data == nil
            case field_conf['on_nil']
              when nil, 'fail'
                raise ArgumentError, "Event data is nil by key `#{key}` in path `#{raw_path}`. event: #{event}"
              when 'ignore'
                return
              when 'ignore warn'
                @logger.warn("Event data is nil by key `#{key}` in path `#{raw_path}. event: #{event}")
                return
              when 'default'
                data = get_default_value(field_conf)
                needs_transformation = false # default values are already transformed
              when 'default warn'
                @logger.warn("Event data is nil by key `#{key}` in path `#{raw_path}. event: #{event}")
                data = get_default_value(field_conf)
                needs_transformation = false # default values are already transformed
              else
                raise ArgumentError, "Invalid configuration. Invalid on_nil value `#{field_conf['on_nil']}` in column config `#{field_conf}`"
            end
            break
          end
        end
        if needs_transformation
          begin
            data = type_transformation[data, event]
          rescue Exception => e
            case field_conf['on_invalid']
              when nil, 'fail'
                raise ArgumentError, "Event data by key `#{raw_path}` is invalid for type `#{type}`, error: #{e.message}, event: #{event}"
              when 'ignore'
                return
              when 'ignore warn'
                @logger.warn("Event data by key `#{raw_path}` is invalid for type `#{type}`, error: #{e.message}, event: #{event}")
                return
              when 'default'
                data = get_default_value(field_conf)
              when 'default warn'
                @logger.warn("Event data by key `#{raw_path}` is invalid for type `#{type}`, error: #{e.message}, event: #{event}")
                data = get_default_value(field_conf)
              else
                raise ArgumentError, "Invalid configuration. Invalid on_invalid value `#{field_conf['on_invalid']}` in column config `#{field_conf}`"
            end
          end
        end
        return data
      end
    end

    # Creates transformation of data to a given cassandra type
    # Generally type transformation does not define default behavior, but it may return udt transformation which will
    def create_type_transformation(type, full_key_path)
      if @simple_type_transformations.has_key?(type)
        return @simple_type_transformations[type]
      elsif /^list<(.*)>$/.match(type)
        type_transformation = create_type_transformation($1, "#{full_key_path}[#list#]")
        return lambda { |data, event|
          converted_items = []
          data.each { |data_item| converted_items << type_transformation[data_item, event] }
          return converted_items
        }
      elsif /^set<(.*)>$/.match(type)
        type_transformation = create_type_transformation($1.strip, "#{full_key_path}[#set#]")
        return lambda { |data, event|
          converted_items = ::Set.new
          data.each { |data_item| converted_items << type_transformation[data_item, event] }
          return converted_items
        }
      elsif /^map<(.*),(.*)>$/.match(type)
        key_type_transformation = create_type_transformation($1.strip, "#{full_key_path}[#map_key#]")
        value_type_transformation = create_type_transformation($2.strip, "#{full_key_path}[#map_value#]")
        return lambda { |data, event|
          converted_items = {}
          data.each { |key, value|
            transformed_key = key_type_transformation[key, event]
            transformed_value = value_type_transformation[value, event]
            converted_items[transformed_key] = transformed_value
          }
          return converted_items
        }
      elsif @udt_configs.has_key?(type)
        return create_udt_transformation(@udt_configs[type], full_key_path)
      else
        raise "Unknown cassandra_type #{type}"
      end
    end

    def get_default_value(field_conf)
      type = field_conf['type']
      default = field_conf['default']
      if default == nil
        return get_default_value_by_type(type)
      else
        if @simple_type_transformations.has_key?(type)
          return @simple_type_transformations[type][default, nil]
        else
          raise ArgumentError, "Invalid configuration. Found explicitly configured default value `#{default}` in config `#{field_conf}` with type `#{type}`. Explicit default values are not supported for collections or user defined types"
        end
      end
    end

    def get_default_value_by_type(type)
      case type
        when 'float', 'int', 'varint', 'bigint', 'double', 'counter', 'timestamp'
          return @simple_type_transformations[type][0, nil]
        when 'timeuuid'
          return @simple_type_transformations[type]['00000000-0000-0000-0000-000000000000', nil]
        when 'inet'
          return @simple_type_transformations[type]['0.0.0.0', nil]
        when /^map<.*>$/
          return {}
        when /^list<.*>$/
          return []
        when /^set<.*>$/
          return Set.new
        else
          raise ArgumentError, "Unable to provide a default value for type #{type}"
      end
    end
  end
end end end
