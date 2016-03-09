# encoding: utf-8
require_relative "./integration_helper"
require "logstash/outputs/cassandra_output"

describe "client create actions", :integration => true do
  before(:all) do
    get_session().execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    get_session().execute("
      CREATE TABLE test.first(
        text_column     text,
        timeuuid_column timeuuid,
        int_column      int,
        PRIMARY KEY     (text_column)
      );")
    get_session().execute("
      CREATE TABLE test.second(
        text_column     text,
        timeuuid_column timeuuid,
        int_column      int,
        PRIMARY KEY     (text_column)
      );")
  end

  before(:each) do
    get_session().execute("TRUNCATE test.first")
    get_session().execute("TRUNCATE test.second")
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

  # TODO: add integration tests here (docker, longhorseman, et al)
  # pushing a single event
  # pushing a set of events
  # pushing to a few tables
  it "properly creates a single event" do
    sut = get_sut()
    sut.register()
    sut.receive(LogStash::Event.new(
        "text_field" => "some text",
        "timeuuid_field" => "00000000-0000-0000-0000-000000000000",
        "int_field" => "345",
        "cassandra_table" => "first",
        "cassandra_filter" => [
          { "event_key" => "text_field",     "column_name" => "text_column" },
          { "event_key" => "timeuuid_field", "column_name" => "timeuuid_column", "cassandra_type" => "timeuuid" },
          { "event_key" => "int_field",      "column_name" => "int_column", "cassandra_type" => "int" }
    ] ))
    sut.flush()

    result = get_session().execute("SELECT * FROM test.first")
    print 'done'
  end
end
