# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/safe_submitter"

RSpec.describe LogStash::Outputs::Cassandra::SafeSubmitter do
  let(:sut) { LogStash::Outputs::Cassandra::SafeSubmitter }
  let(:default_options) {
    logger = double()
    allow(logger).to(receive(:debug))
    {
      "logger" => logger,
      "cassandra" => double(),
      "username" => "a user",
      "password" => "a password",
      "hosts" => "some host",
      "port" => 9042,
      "consistency" => "one",
      "request_timeout" => 10,
      "retry_policy" => "default",
      "concrete_retry_policy" => ::Cassandra::Retry::Policies::Default,
      "keyspace" => "the final frontier"
    }
  }

  def setup_session_double(options)
    session_double = double()
    cluster_double = double()
    expect(cluster_double).to(receive(:connect)).with(options["keyspace"]).and_return(session_double)
    expect(options["cassandra"]).to(receive(:cluster).with(
      username: options["username"],
      password: options["password"],
      hosts: options["hosts"],
      port: options["port"],
      consistency: options["consistency"].to_sym,
      timeout: options["request_timeout"],
      retry_policy: options["concrete_retry_policy"],
      logger: options["logger"]
    )).and_return(cluster_double)
    return { :session_double => session_double }
  end

  describe "init" do
    it "properly inits the cassandra session" do
      setup_session_double(default_options)

      sut.new(default_options)
    end

    [
        { :name => "default",                 :concrete_retry_policy => ::Cassandra::Retry::Policies::Default },
        { :name => "downgrading_consistency", :concrete_retry_policy => ::Cassandra::Retry::Policies::DowngradingConsistency },
        { :name => "failthrough",             :concrete_retry_policy => ::Cassandra::Retry::Policies::Fallthrough }
    ].each { |mapping|
      it "supports the #{mapping["class"]} retry policy by passing #{mapping["name"]} as the retry_policy" do
        options = default_options.update({ "retry_policy" => mapping[:name], "concrete_retry_policy" => mapping[:concrete_retry_policy] })
        setup_session_double(options)

        sut.new(options)
      end
    }
  end

  describe "execution" do
    let(:one_action) {{
      "table" => "a_table",
      "data" => {
        "a_column" => "a_value",
        "another_column" => "another_value"
      }
    }}
    let(:expected_query_for_one_action) { "INSERT INTO a_table (a_column, another_column)\nVALUES (?, ?)" }
    let(:another_action) {{
      "table" => "another_table",
      "data" => {
          "a_column" => "a_value",
          "another_column" => "another_value",
          "a_third_column" => "another_value"
      }
    }}
    let(:expected_query_for_another_action) { "INSERT INTO another_table (a_column, another_column, a_third_column)\nVALUES (?, ?, ?)" }

    def setup_batch_and_session_doubles()
      session_double = setup_session_double(default_options)[:session_double]
      batch_double = double()
      expect(session_double).to(receive(:batch).and_yield(batch_double).at_least(:once)).and_return(batch_double)
      expect(session_double).to(receive(:execute).with(batch_double).at_least(:once))
      return { :batch_double => batch_double, :session_double => session_double }
    end

    it "prepares and executes the query" do
      doubles = setup_batch_and_session_doubles()
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action)).and_return("eureka")
      expect(doubles[:batch_double]).to(receive(:add).with("eureka", ["a_value", "another_value"]))
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action])
    end

    it "caches the generated query" do
      doubles = setup_batch_and_session_doubles()
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action).once).and_return("eureka")
      expect(doubles[:batch_double]).to(receive(:add).with("eureka", ["a_value", "another_value"]).twice)
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action, one_action])
    end

    it "does not confuse between a new query and cached queries" do
      doubles = setup_batch_and_session_doubles()
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action).once).and_return("eureka")
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_another_action).once).and_return("great scott")
      expect(doubles[:batch_double]).to(receive(:add).with("eureka", ["a_value", "another_value"]))
      expect(doubles[:batch_double]).to(receive(:add).with("great scott", ["a_value", "another_value", "another_value"]))
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action, another_action])
    end
  end
end
