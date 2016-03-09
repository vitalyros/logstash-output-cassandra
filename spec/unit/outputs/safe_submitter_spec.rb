# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/safe_submitter"

RSpec.describe LogStash::Outputs::Cassandra::SafeSubmitter do
  let(:sut) { LogStash::Outputs::Cassandra::SafeSubmitter }
  let(:default_opts) {{
      "logger" => double(),
      "username" => "a user",
      "password" => "a password",
      "hosts" => "some host",
      "consistency" => "one",
      "request_timeout" => 10,
      "retry_policy" => "default",
      "keyspace" => "the final frontier"
  }}

  describe "init" do
    it "properly inits the cassandra session"
    it "supports the ... retry policy by passing ... as the retry_policy"
  end

  describe "execution" do
    it "prepares and executes the query"
    it "caches the generated query"
    it "does not confuse between a new query and cached queries"
  end
end
