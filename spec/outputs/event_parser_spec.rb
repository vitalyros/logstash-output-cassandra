require_relative "../cassandra_spec_helper"
require "logstash/outputs/cassandra/event_parser"

RSpec.describe LogStash::Outputs::Cassandra::EventParser do
  let(:sut) { LogStash::Outputs::Cassandra::EventParser }
  let(:default_opts) {{
    'logger' => double(),
    'table' => 'dummy',
    'filter_transform_event_key' => nil,
    'filter_transform' => nil,
    'hints' => {},
    'ignore_bad_values' => false
  }}
  let(:sample_event) { LogStash::Event.new("message" => "sample message here") }

  describe "table name parsing" do
    it "leaves regular table names unchanged" do
      sut_instance = sut().new(default_opts.update({ "table" => "simple" }))
      action = sut_instance.parse(sample_event)
      expect(action["table"]).to(eq("simple"))
    end
    # @table
    # => regular table name
    # => event table name
  end

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
