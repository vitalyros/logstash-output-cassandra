# encoding: utf-8
require_relative "./integration_helper"
require "logstash/outputs/cassandra_output"

describe "client create actions", :integration => true do
  before(:all) do
    get_session().execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    get_session().execute("
      CREATE TABLE test.simple(
        text_column      text,
        int_column       int,
        PRIMARY KEY      (text_column)
      );")
    get_session().execute("
      CREATE TABLE test.complex(
        timestamp_column timestamp,
        inet_column      inet,
        float_column     float,
        varchar_column   varchar,
        text_column      text,
        blob_column      blob,
        ascii_column     ascii,
        bigint_column    bigint,
        int_column       int,
        varint_column    varint,
        boolean_column   boolean,
        decimal_column   decimal,
        double_column    double,
        timeuuid_column  timeuuid,
        set_column       set<timeuuid>,
        PRIMARY KEY      (text_column)
      );")
  end

  before(:each) do
    get_session().execute("TRUNCATE test.simple")
    get_session().execute("TRUNCATE test.complex")
  end

  def get_sut()
    options = {
      "hosts" => [get_host_ip()],
      "port" => get_port(),
      "keyspace" => "test",
      "table" => "%{[cassandra_table]}",
      "username" => "cassandra",
      "password" => "cassandra",
      "filter_transform_event_key" => "cassandra_filter"
    }
    sut = LogStash::Outputs::CassandraOutput.new(options)
    return sut
  end

  it "properly creates a single event" do
    sut = get_sut()
    sut.register()
    sut.receive(LogStash::Event.new(
        "text_field" => "some text",
        "int_field" => "345",
        "cassandra_table" => "simple",
        "cassandra_filter" => [
          { "event_key" => "text_field", "column_name" => "text_column" },
          { "event_key" => "int_field", "column_name" => "int_column", "cassandra_type" => "int" }
    ] ))
    sut.flush()

    result = get_session().execute("SELECT * FROM test.simple")
    expect(result.size).to((eq(1)))
    result.each { |row|
      expect(row["text_column"]).to(eq("some text"))
      expect(row["int_column"]).to(eq(345))
    }
  end

  it "properly creates all column types"
  it "properly works with counter columns"
  it "properly adds multiple events to multiple tables in the same batch"
end
