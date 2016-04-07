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
      "protocol_version" => 3,
      "hosts" => "some host",
      "port" => 9042,
      "consistency" => "one",
      "request_timeout" => 10,
      "retry_policy" => { "type" => "default" },
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
      protocol_version: options["protocol_version"],
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
        { :setting => { "type" => "default" },                 :concrete_retry_policy => ::Cassandra::Retry::Policies::Default },
        { :setting => { "type" => "downgrading_consistency" }, :concrete_retry_policy => ::Cassandra::Retry::Policies::DowngradingConsistency },
        { :setting => { "type" => "failthrough" },             :concrete_retry_policy => ::Cassandra::Retry::Policies::Fallthrough },
        { :setting => { "type" => "backoff", "backoff_type" => "**", "backoff_size" => 2, "retry_limit" => 10 },
                                                               :concrete_retry_policy => ::Cassandra::Retry::Policies::Backoff }
    ].each { |mapping|
      it "supports the #{mapping[:concrete_retry_policy]} retry policy by passing #{mapping[:setting]["type"]} as the retry_policy" do
        options = default_options.update({ "retry_policy" => mapping[:setting], "concrete_retry_policy" => mapping[:concrete_retry_policy] })
        setup_session_double(options)

        sut.new(options)
      end
    }

    it "properly initializes the backoff retry policy" do
      retry_policy_config = { "type" => "backoff", "backoff_type" => "**", "backoff_size" => 2, "retry_limit" => 10 }
      expected_policy = double()
      options = default_options.update({ "retry_policy" => retry_policy_config, "concrete_retry_policy" => expected_policy })
      expect(::Cassandra::Retry::Policies::Backoff).to(receive(:new).with({
        "backoff_type" => options["retry_policy"]["backoff_type"], "backoff_size" => options["retry_policy"]["backoff_size"],
        "retry_limit" => options["retry_policy"]["retry_limit"], "logger" =>  options["logger"]}).and_return(expected_policy))
      setup_session_double(options)

      sut.new(options)
    end
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

    def generate_future_double()
      future_double = double()
      expect(future_double).to(receive(:join))
      expect(future_double).to(receive(:on_failure))
      expect(future_double).to(receive(:on_complete))
      return future_double
    end

    it "prepares and executes the query" do
      doubles = setup_session_double(default_options)
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action)).and_return("eureka")
      expect(doubles[:session_double]).to(receive(:execute_async).with("eureka", :arguments => one_action["data"].values)).and_return(generate_future_double())
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action])
    end

    it "caches the generated query" do
      doubles = setup_session_double(default_options)
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action).once).and_return("eureka")
      2.times {
        expect(doubles[:session_double]).to(receive(:execute_async).with("eureka", :arguments => one_action["data"].values)).and_return(generate_future_double())
      }
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action, one_action])
    end

    it "does not confuse between a new query and cached queries" do
      doubles = setup_session_double(default_options)
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_one_action).once).and_return("eureka")
      expect(doubles[:session_double]).to(receive(:prepare).with(expected_query_for_another_action).once).and_return("great scott")
      expect(doubles[:session_double]).to(receive(:execute_async).with("eureka", :arguments => one_action["data"].values)).and_return(generate_future_double())
      expect(doubles[:session_double]).to(receive(:execute_async).with("great scott", :arguments => another_action["data"].values)).and_return(generate_future_double())
      sut_instance = sut.new(default_options)

      sut_instance.submit([one_action, another_action])
    end
  end
end
