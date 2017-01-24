# encoding: utf-8
require_relative '../../cassandra_spec_helper'
require 'logstash/outputs/cassandra/event_parser'

RSpec.describe LogStash::Outputs::Cassandra::EventParser do
  let(:sut) { LogStash::Outputs::Cassandra::EventParser }
  let(:default_opts) {
    logger = double
    allow(logger).to receive(:debug?).and_return(true)
    allow(logger).to receive(:warn?).and_return(true)
    allow(logger).to receive(:error?).and_return(true)
    allow(logger).to(receive(:debug)) { |*args| puts "DEBUG: #{args}"}
    allow(logger).to(receive(:warn)) { |*args| puts "WARN: #{args}"}
    allow(logger).to(receive(:error)) { |*args| puts "ERROR: #{args}"}
    return {
      'logger' => logger,
      'table' => 'dummy',
      'user_defined_types' => [],
      'columns' => []
    }
  }
  let(:opts) { default_opts }
  let(:sut_instance) { sut.new(opts)}
  let(:sample_event) { LogStash::Event.new('message' => 'sample message here') }
  let(:field_name) { 'a_field' }
  let(:column_name) { 'a_column' }

  describe 'simple type mapping' do
    context 'when correct values are mapped' do
      [
        { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => Time::parse('1979-07-27 00:00:00 +0300') },
        { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => '1982-05-04 00:00:00 +0300', expected: Time::parse('1982-05-04 00:00:00 +0300') },
        { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => 1457606758, expected: Time.at(1457606758) },
        { :name => 'inet',      :type => ::Cassandra::Types::Inet,      :value => '0.0.0.0' },
        { :name => 'float',     :type => ::Cassandra::Types::Float,     :value => '10.15' },
        { :name => 'text',      :type => ::Cassandra::Types::Text,      :value => 'some text' },
        { :name => 'blob',      :type => ::Cassandra::Types::Blob,      :value => '12345678' },
        { :name => 'ascii',     :type => ::Cassandra::Types::Ascii,     :value => 'some ascii' },
        { :name => 'bigint',    :type => ::Cassandra::Types::Bigint,    :value => '100' },
        { :name => 'counter',   :type => ::Cassandra::Types::Counter,   :value => '15' },
        { :name => 'int',       :type => ::Cassandra::Types::Int,       :value => '123' },
        { :name => 'varint',    :type => ::Cassandra::Types::Varint,    :value => '345' },
        { :name => 'boolean',   :type => ::Cassandra::Types::Boolean,   :value => 'true' },
        { :name => 'decimal',   :type => ::Cassandra::Types::Decimal,   :value => '0.12E2' },
        { :name => 'double',    :type => ::Cassandra::Types::Double,    :value => '123.65' },
        { :name => 'timeuuid',  :type => ::Cassandra::Types::Timeuuid,  :value => '00000000-0000-0000-0000-000000000000' }
      ].each { |mapping|
        it "properly maps #{mapping[:name]} to #{mapping[:type]}" do
          sut_instance = sut.new(default_opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name] }] }))
          sample_event[field_name] = mapping[:value]

          action = sut_instance.parse(sample_event)

          expected_value = mapping.has_key?(:expected) ? mapping[:expected] : mapping[:value]
          expect(action['data'][column_name].to_s).to(eq(expected_value.to_s))
        end
      }
    end
    describe 'use of implicit default value on nil' do
      [
        { :name => 'timestamp', :expected => ::Cassandra::Types::Timestamp.new(Time.at(0)) },
        { :name => 'inet', :expected => '0.0.0.0' },
        { :name => 'float', :expected => 0.0 },
        { :name => 'bigint', :expected => 0 },
        { :name => 'counter', :expected => 0 },
        { :name => 'int', :expected => 0 },
        { :name => 'varint', :expected => 0 },
        { :name => 'double', :expected => 0.0 },
        { :name => 'timeuuid', :expected => '00000000-0000-0000-0000-000000000000' },
        { :name => 'decimal', :error_expected => true },
        { :name => 'text', :error_expected => true },
        { :name => 'blob', :error_expected => true },
        { :name => 'ascii', :error_expected => true },
        { :name => 'boolean', :error_expected => true }
      ].each { |mapping|
        it 'maps to implicit default value' do
          sut_instance = sut.new(default_opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_nil' => 'default' }] }))

          action = sut_instance.parse(sample_event)
          if mapping[:error_expected]
            expect(action).to eq(nil)
          end
          if mapping.has_key?(:expected)
            expect(action['data'][column_name].to_s).to(eq(mapping[:expected].to_s))
          end
        end
      }
    end
    describe 'use of implicit default value on parse error' do
      [
        { :name => 'timestamp', :value => 'abc', :expected => ::Cassandra::Types::Timestamp.new(Time.at(0)) },
        { :name => 'inet', :value => 'abc', :expected => '0.0.0.0' },
        { :name => 'float', :value => 'abc', :expected => 0.0 },
        { :name => 'bigint', :value => 'abc', :expected => 0 },
        { :name => 'counter', :value => 'abc', :expected => 0 },
        { :name => 'int', :value => 'abc', :expected => 0 },
        { :name => 'varint', :value => 'abc', :expected => 0 },
        { :name => 'double', :value => 'abc', :expected => 0.0 },
        { :name => 'timeuuid', :value => 'abc', :expected => '00000000-0000-0000-0000-000000000000' }
      ].each { |mapping|
        it 'maps to implicit default value' do
          sut_instance = sut.new(default_opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_invalid' => 'default' }] }))
          sample_event[field_name] = mapping[:value]

          action = sut_instance.parse(sample_event)

          expect(action['data'][column_name].to_s).to(eq(mapping[:expected].to_s))
        end
      }
    end
    describe 'use of explicit default value on nil' do
      [
        { :name => 'timestamp', :default => 12345, :type => ::Cassandra::Types::Timestamp, :expected => ::Cassandra::Types::Timestamp.new(Time.at(12345)) },
        { :name => 'inet', :default => '8.8.8.8', :type => ::Cassandra::Types::Inet },
        { :name => 'float', :default => 10.15, :type => ::Cassandra::Types::Float },
        { :name => 'text', :default => 'some text', :type => ::Cassandra::Types::Text },
        { :name => 'blob', :default => '12345678', :type => ::Cassandra::Types::Blob },
        { :name => 'ascii', :default => 'some ascii', :type => ::Cassandra::Types::Ascii },
        { :name => 'bigint', :default => 100, :type => ::Cassandra::Types::Bigint },
        { :name => 'counter', :default => 15, :type => ::Cassandra::Types::Counter },
        { :name => 'int', :default => 123, :type => ::Cassandra::Types::Int },
        { :name => 'varint', :default => 345, :type => ::Cassandra::Types::Varint },
        { :name => 'boolean', :default => true, :type => ::Cassandra::Types::Boolean },
        { :name => 'decimal', :default => '0.12E2', :type => ::Cassandra::Types::Decimal },
        { :name => 'double', :default => 123.65, :type => ::Cassandra::Types::Double },
        { :name => 'timeuuid', :default => '6ad2be8c-dd70-11e6-bf26-cec0c932ce01', :type => ::Cassandra::Types::Timeuuid },
      ].each { |mapping|
        it 'maps to the explicitly configured default value' do
          sut_instance = sut.new(default_opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_nil' => 'default', 'default' => mapping[:default]}] }))

          action = sut_instance.parse(sample_event)

          expected = mapping[:expected] != nil ?  mapping[:expected] : mapping[:type].new(mapping[:default])
          expect(action['data'][column_name]).to(eq(expected))
        end
      }
    end
    describe 'use of explicit default value on parse error' do
      [
        { :name => 'timestamp', :value => 'abc', :default => 12345, :type => ::Cassandra::Types::Timestamp, :expected => ::Cassandra::Types::Timestamp.new(Time.at(12345)) },
        { :name => 'inet', :value => 'abc', :default => '8.8.8.8', :type => ::Cassandra::Types::Inet },
        { :name => 'float', :value => 'abc', :default => 10.15, :type => ::Cassandra::Types::Float },
        { :name => 'bigint', :value => 'abc', :default => 100, :type => ::Cassandra::Types::Bigint },
        { :name => 'counter', :value => 'abc', :default => 15, :type => ::Cassandra::Types::Counter },
        { :name => 'int', :value => 'abc', :default => 123, :type => ::Cassandra::Types::Int },
        { :name => 'varint', :value => 'abc', :default => 345, :type => ::Cassandra::Types::Varint },
        { :name => 'double', :value => 'abc', :default => 123.65, :type => ::Cassandra::Types::Double },
        { :name => 'timeuuid', :value => 'abc', :default => '6ad2be8c-dd70-11e6-bf26-cec0c932ce01', :type => ::Cassandra::Types::Timeuuid },
      ].each { |mapping|
        it 'maps to the explicitly configured default value' do
          sut_instance = sut.new(default_opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_invalid' => 'default', 'default' => mapping[:default] }] }))
          sample_event[field_name] = mapping[:value]

          action = sut_instance.parse(sample_event)

          expected = mapping[:expected] != nil ?  mapping[:expected] : mapping[:type].new(mapping[:default])
          expect(action['data'][column_name]).to(eq(expected))
        end
        }
    end
  end

  describe 'list mapping' do
    let(:opts) {
      opts = default_opts.clone
      opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name] }]})
      opts
    }
    context 'when the data field is full' do
      let(:mapping) { { :name => "list<int>", :value => ['1','2','3'], :expected => [1,2,3] } }
      it 'correctly maps to full set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name].to_s).to(eq(mapping[:expected].to_s))
      end
    end
    context 'when the data field is empty' do
      let(:mapping) { { :name => "list<int>", :value => [], :expected => [] } }
      it 'maps to empty list' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name].to_s).to(eq(mapping[:expected].to_s))
      end
    end
    describe 'use of implicit default value on nil' do
      let(:mapping) { { :name => "list<int>", :value => nil, :expected => [] } }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_nil' => 'default' }]})
        opts
      }
      it 'maps to empty list' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
    describe 'use of implicit default value on parse failure' do
      let(:mapping) { { :name => "list<int>", :value => 'invalid value', :expected => [] } }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_invalid' => 'default' }]})
        opts
      }
      it 'maps to an empty list' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
  end

  describe 'set mapping' do
    let(:opts) {
      opts = default_opts.clone
      opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name] }]})
      opts
    }
    context 'when array is full' do
      let(:mapping) { { :name => "set<int>", :value => ['1','2','3','3'], :expected => Set.new([1,2,3]) } }
      it 'correctly maps to full set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to(eq(mapping[:expected]))
      end
    end
    context 'when array is empty' do
      let(:mapping) { { :name => "set<int>", :value => [], :expected => Set.new } }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to(eq(mapping[:expected]))
      end
    end
    describe 'use of implicit default value on nil' do
      let(:mapping) { { :name => "set<int>", :value => nil, :expected => Set.new } }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_nil' => 'default' }]})
        opts
      }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
    describe 'use of implicit default value on parse failure' do
      let(:mapping) { { :name => "set<int>", :value => 'invalid value', :expected => Set.new} }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_invalid' => 'default' }]})
        opts
      }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
  end

  describe 'map mapping' do
    let(:opts) {
      opts = default_opts.clone
      opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name] }]})
      opts
    }
    context 'when map is full' do
      let(:mapping) { { :name => "map<int, text>", :value => {1 => 'one', 2 => 'two', 3 => 'three'} } }
      it 'correctly maps to full set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to(eq(mapping[:value ]))
      end
    end
    context 'when map is empty' do
      let(:mapping) { { :name => "map<int, text>", :value => {} } }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to(eq(mapping[:value]))
      end
    end
    describe 'use of implicit default value on nil' do
      let(:mapping) { { :name => "map<int, text>", :value => nil, :expected => {} } }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_nil' => 'default' }]})
        opts
      }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
    describe 'use of implicit default value on parse failure' do
      let(:mapping) { { :name => "map<int, text>", :value => 'invalid value', :expected => {} } }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => mapping[:name], 'on_invalid' => 'default' }]})
        opts
      }
      it 'maps to empty set' do
        sample_event[field_name] = mapping[:value]
        action = sut_instance.parse(sample_event)

        expect(action['data'][column_name]).to eq(mapping[:expected])
      end
    end
  end

  describe 'user defined type mapping' do
    describe 'single udt mapping' do
      let(:udt) {{
        'name' => 'test_udt',
        'fields' => [
          { 'key' => 'field_1', 'type' => 'int' },
          { 'key' => 'field_2', 'type' => 'text' },
          { 'key' => 'field_3', 'type' => 'timestamp' }
        ]
      }}
      let(:udt_value) {{
        'field_1' => 123,
        'field_2' => 'test',
        'field_3' => '1982-05-04 00:00:00 +0300'
      }}
      let(:mapped_udt_value) {
        ::Cassandra::UDT.new({
          'field_1' => ::Cassandra::Types::Int.new(123),
          'field_2' => ::Cassandra::Types::Text.new('test'),
          'field_3' => ::Cassandra::Types::Timestamp.new(Time::parse('1982-05-04 00:00:00 +0300'))
        })
      }
      let(:event) {
        event = sample_event.clone
        event[field_name] = udt_value
        event
      }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'user_defined_types' => [udt]})
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => udt['name'] }]})
        opts
      }
      it 'correctly maps to a udt' do
        action = sut_instance.parse(event)

        expect(action['data'][column_name]).to eq(mapped_udt_value)
      end
    end

    describe 'udt default field values' do
      let(:udt) {{
        'name' => 'test_udt',
        'fields' => [
          { 'key' => 'field_1', 'type' => 'int', 'on_invalid' => 'default'},
          { 'key' => 'field_2', 'type' => 'int', 'on_invalid' => 'default warn'},
          { 'key' => 'field_3', 'type' => 'int', 'on_invalid' => 'default', 'default' => 1},
          { 'key' => 'field_4', 'type' => 'int', 'on_invalid' => 'default warn', 'default' => 2},
          { 'key' => 'field_5', 'type' => 'int', 'on_invalid' => 'ignore'},
          { 'key' => 'field_6', 'type' => 'int', 'on_invalid' => 'ignore warn'},
          { 'key' => 'field_7', 'type' => 'int', 'on_nil' => 'default' },
          { 'key' => 'field_8', 'type' => 'int', 'on_nil' => 'default warn' },
          { 'key' => 'field_9', 'type' => 'int', 'on_nil' => 'default', 'default' => 3},
          { 'key' => 'field_10', 'type' => 'int', 'on_nil' => 'default warn', 'default' => 4},
          { 'key' => 'field_11', 'type' => 'int', 'on_nil' => 'ignore' },
          { 'key' => 'field_12', 'type' => 'int', 'on_nil' => 'ignore warn' },
        ]
      }}
      let(:udt_value) {{
        'field_1' => 'abc',
        'field_2' => 'abc',
        'field_3' => 'abc',
        'field_4' => 'abc',
        'field_5' => 'abc',
        'field_6' => 'abc',
        'field_7' => nil,
        'field_8' => nil,
        'field_9' => nil,
        'field_10' => nil,
        'field_11' => nil,
        'field_12' => nil
      }}
      let(:mapped_udt_value) {
        ::Cassandra::UDT.new({
          'field_1' => ::Cassandra::Types::Int.new(0),
          'field_2' => ::Cassandra::Types::Int.new(0),
          'field_3' => ::Cassandra::Types::Int.new(1),
          'field_4' => ::Cassandra::Types::Int.new(2),
          'field_5' => nil,
          'field_6' => nil,
          'field_7' => ::Cassandra::Types::Int.new(0),
          'field_8' => ::Cassandra::Types::Int.new(0),
          'field_9' => ::Cassandra::Types::Int.new(3),
          'field_10' => ::Cassandra::Types::Int.new(4),
          'field_11' => nil,
          'field_12' => nil
        })
      }
      let(:event) {
        event = sample_event.clone
        event[field_name] = udt_value
        event
      }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'user_defined_types' => [udt]})
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => udt['name'] }]})
        opts
      }
      it 'correctly maps to a udt' do
        action = sut_instance.parse(event)

        expect(action['data'][column_name]).to eq(mapped_udt_value)
      end
    end

    describe 'nested udt and collection mappings' do
      # Testing a list of udt_3, that contains a map of text => udt_2, which in turn contains a udt_1 field
      let(:udt_1) {{
        'name' => 'udt_1',
        'fields' => [
          { 'key' => 'field_1_1', 'type' => 'int' },
          { 'key' => 'field_1_2', 'type' => 'text' },
          { 'key' => 'field_1_3', 'type' => 'timestamp' }
        ]
      }}
      let(:udt_2) {{
        'name' => 'udt_2',
        'fields' => [
          { 'key' => 'field_2_1', 'type' => 'text' },
          { 'key' => 'field_2_2', 'type' => 'udt_1' }
        ]
      }}
      let(:udt_3) {{
        'name' => 'udt_3',
        'fields' => [
          { 'key' => 'field_3_1', 'type' => 'text' },
          { 'key' => 'field_3_2', 'type' => 'map<text, udt_2>' }
        ]
      }}
      let(:udt_value) {
        [
          {
            'field_3_1' => '1',
            'field_3_2' => {
              'key_1' => {
                'field_2_1' => '1_1',
                'field_2_2' => {
                  'field_1_1' => 123,
                  'field_1_2' => '1_1_1',
                  'field_1_3' => '1982-05-04 00:00:00 +0300'
                }
              },
              'key_2' => {
                'field_2_1' => '1_2',
                'field_2_2' => {
                  'field_1_1' => 456,
                  'field_1_2' => '1_2_1',
                  'field_1_3' => '1987-07-28 00:00:00 +0300'
                }
              }
            }
          },
          {
            'field_3_1' => '2',
            'field_3_2' => { }
          }
        ]
      }
      let(:mapped_udt_value) {
        [
          ::Cassandra::UDT.new({
            'field_3_1' => ::Cassandra::Types::Text.new('1'),
            'field_3_2' => {
              ::Cassandra::Types::Text.new('key_1') => ::Cassandra::UDT.new({
                'field_2_1' => ::Cassandra::Types::Text.new('1_1'),
                'field_2_2' => ::Cassandra::UDT.new({
                  'field_1_1' => ::Cassandra::Types::Int.new(123),
                  'field_1_2' => ::Cassandra::Types::Text.new('1_1_1'),
                  'field_1_3' => ::Cassandra::Types::Timestamp.new(Time::parse('1982-05-04 00:00:00 +0300'))
                })
              }),
              ::Cassandra::Types::Text.new('key_2') => ::Cassandra::UDT.new({
                'field_2_1' => ::Cassandra::Types::Text.new('1_2'),
                'field_2_2' => ::Cassandra::UDT.new({
                  'field_1_1' => ::Cassandra::Types::Int.new(456),
                  'field_1_2' => ::Cassandra::Types::Text.new('1_2_1'),
                  'field_1_3' => ::Cassandra::Types::Timestamp.new(Time::parse('1987-07-28 00:00:00 +0300'))
                })
              })
            }
          }),
          ::Cassandra::UDT.new({
            'field_3_1' => ::Cassandra::Types::Text.new('2'),
            'field_3_2' => {}
          })
        ]
      }
      let(:event) {
        event = sample_event.clone
        event[field_name] = udt_value
        event
      }
      let(:opts) {
        opts = default_opts.clone
        opts.update({ 'user_defined_types' => [udt_1, udt_2, udt_3]})
        opts.update({ 'columns' => [{ 'key' => field_name, 'name' => column_name, 'type' => "list<#{udt_3['name']}>" }]})
        opts
      }
      it 'correctly maps to a udt' do
        action = sut_instance.parse(event)

        expect(action['data'][column_name]).to eq(mapped_udt_value)
      end
    end
  end
end
