# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/safe_submitter"

RSpec.describe LogStash::Outputs::Cassandra::SafeSubmitter do
  let(:sut) { LogStash::Outputs::Cassandra::SafeSubmitter }
  let(:default_options) {{
      "logger" => double(),
      "cassandra" => double(),
      "username" => "a user",
      "password" => "a password",
      "hosts" => "some host",
      "consistency" => "one",
      "request_timeout" => 10,
      "retry_policy" => "default",
      "concrete_retry_policy" => ::Cassandra::Retry::Policies::Default,
      "keyspace" => "the final frontier"
  }}

  describe "init" do
    def setup_cassandra_double(options)
      session_double = double()
      cluster_double = double()
      expect(cluster_double).to(receive(:connect)).with(options["keyspace"]).and_return(session_double)
      expect(options["cassandra"]).to(receive(:cluster).with(
        username: options["username"],
        password: options["password"],
        hosts: options["hosts"],
        consistency: options["consistency"].to_sym,
        timeout: options["request_timeout"],
        retry_policy: options["concrete_retry_policy"],
        logger: options["logger"]
      )).and_return(cluster_double)
    end

    it "properly inits the cassandra session" do
      setup_cassandra_double(default_options)

      sut.new(default_options)
    end

    [
        { :name => "default",                 :concrete_retry_policy => ::Cassandra::Retry::Policies::Default },
        { :name => "downgrading_consistency", :concrete_retry_policy => ::Cassandra::Retry::Policies::DowngradingConsistency },
        { :name => "failthrough",             :concrete_retry_policy => ::Cassandra::Retry::Policies::Fallthrough }
    ].each { |mapping|
      it "supports the #{mapping["class"]} retry policy by passing #{mapping["name"]} as the retry_policy" do
        options = default_options.update({ "retry_policy" => mapping[:name], "concrete_retry_policy" => mapping[:concrete_retry_policy] })
        setup_cassandra_double(options)

        sut.new(options)
      end
    }
  end

  describe "execution" do
    it "prepares and executes the query"
    it "caches the generated query"
    it "does not confuse between a new query and cached queries"
  end
end
