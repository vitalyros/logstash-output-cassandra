require_relative "../cassandra_spec_helper"
require "logstash/outputs/cassandra/event_parser"

describe LogStash::Outputs::Cassandra::EventParser do
  # @table
  # => regular table name
  # => event table name

  # @filter_transform_event_key
  # => get from event

  # @filter_transform
  # => malformed
  # => single
  # => multiple
  # => without type
  # => with type

  # @hints
  # => does nothing for none
  # => hints what it knows
  # => fails for unknown types

  # @ignore_bad_values
  # => fails on bad values if false
  # => if true
  # =>    defaults what it can
  # =>    skips what it cant
end
