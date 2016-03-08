# encoding: utf-8
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

    it "parses table names with data from the event" do
      sut_instance = sut().new(default_opts.update({ "table" => "%{[a_field]}" }))
      sample_event["a_field"] = "a_value"
      action = sut_instance.parse(sample_event)
      expect(action["table"]).to(eq("a_value"))
    end
  end

  # @filter_transform
  describe "filter transforms" do
    describe "from config" do
      describe "malformed configurations" do
        it "fails if the transform has no event_data setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "column_name" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end

        it "fails if the transform has no column_name setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "event_data" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end
      end

      # => single

      # => multiple
      # => without type
      # => with type
    end

    describe "from event" do
      # @filter_transform_event_key
      # => get from event
    end
  end

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
