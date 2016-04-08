# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/backoff_retry_policy"

RSpec.shared_examples "limited parameterized backoff" do
  let(:sut) { ::Cassandra::Retry::Policies::Backoff }

  it "stops once the max retries are reached"
  it "waits between retries"
  it "allows for exponential backoffs"
  it "allows for linear backoffs"
end

RSpec.describe ::Cassandra::Retry::Policies::Backoff do
  let(:sut) { ::Cassandra::Retry::Policies::Backoff }

  describe "#read_timeout" do
    include_examples "limited parameterized backoff"

    it "tries again if the result did not arrive, but the required acks arrived"
    it "tries the next host, if retries are left"
  end

  describe "#write_timeout" do
    include_examples "limited parameterized backoff"

    it "tries the next host if no acks were recieved (there is an undelying assumption that the query is idempotent)"
    it "retries if the query was a logged batch"
  end

  describe "#unavailable" do
    include_examples "limited parameterized backoff"

    it "tries the next host"
  end
end
