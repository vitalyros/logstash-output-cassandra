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

  describe "filter transforms" do
    describe "from config" do
      describe "malformed configurations" do
        it "fails if the transform has no event_data setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "column_name" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end

        it "fails if the transform has no column_name setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end
      end

      describe "properly configured" do
        it "maps the event key to the column" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column" }] }))
          sample_event["a_field"] = "a_value"
          action = sut_instance.parse(sample_event)
          expect(action["data"]["a_column"]).to(eq("a_value"))
        end

        it "works with multiple filter transforms" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column" }, { "event_key" => "another_field", "column_name" => "a_different_column" }] }))
          sample_event["a_field"] = "a_value"
          sample_event["another_field"] = "a_second_value"
          action = sut_instance.parse(sample_event)
          expect(action["data"]["a_column"]).to(eq("a_value"))
          expect(action["data"]["a_different_column"]).to(eq("a_second_value"))
        end

        it "allows for event specific event keys" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "%{[pointer_to_another_field]}", "column_name" => "a_column" }] }))
          sample_event["pointer_to_another_field"] = "another_field"
          sample_event["another_field"] = "a_value"
          action = sut_instance.parse(sample_event)
          expect(action["data"]["a_column"]).to(eq("a_value"))
        end

        it "allows for event specific column names" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "%{[pointer_to_another_field]}" }] }))
          sample_event["a_field"] = "a_value"
          sample_event["pointer_to_another_field"] = "a_different_column"
          action = sut_instance.parse(sample_event)
          expect(action["data"]["a_different_column"]).to(eq("a_value"))
        end

      end

      describe "cassandra type mapping" do
        [
          { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => Time::parse("1970-01-01 00:00:00") },
          { :name => 'inet', :type => ::Cassandra::Types::Inet, :value => "0.0.0.0" },
          { :name => 'float', :type => ::Cassandra::Types::Float, :value => "10.15" },
          { :name => 'varchar', :type => ::Cassandra::Types::Varchar, :value => "a varchar" },
          { :name => 'text', :type => ::Cassandra::Types::Text, :value => "some text" },
          { :name => 'blob', :type => ::Cassandra::Types::Blob, :value => "12345678" },
          { :name => 'ascii', :type => ::Cassandra::Types::Ascii, :value => "some ascii" },
          { :name => 'bigint', :type => ::Cassandra::Types::Bigint, :value => "100" },
          { :name => 'counter', :type => ::Cassandra::Types::Counter, :value => "15" },
          { :name => 'int', :type => ::Cassandra::Types::Int, :value => "123" },
          { :name => 'varint', :type => ::Cassandra::Types::Varint, :value => "345" },
          { :name => 'boolean', :type => ::Cassandra::Types::Boolean, :value => "true" },
          { :name => 'decimal', :type => ::Cassandra::Types::Decimal, :value => "0.12E2" },
          { :name => 'double', :type => ::Cassandra::Types::Double, :value => "123.65" },
          { :name => 'timeuuid', :type => ::Cassandra::Types::Timeuuid, :value => "00000000-0000-0000-0000-000000000000" }
        ].each { |mapping|
          # NOTE: this is not the best test there is, but it is the best / simplest I could think of :/
          it "properly maps #{mapping[:name]} to #{mapping[:type]}" do
            sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column", "cassandra_type" => mapping[:name] }] }))
            sample_event["a_field"] = mapping[:value]
            action = sut_instance.parse(sample_event)
            expect(action["data"]["a_column"].to_s).to(eq(mapping[:value].to_s))
          end
        }

        it "properly maps sets to their specific set types"
        it "allows for event specific cassandra types"
      end
    end

    describe "from event" do
      it "obtains the filter transform from the event if defined"
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
