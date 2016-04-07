# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/backoff_retry_policy"

RSpec.describe ::Cassandra::Retry::Policies::Backoff do
  let(:sut) { ::Cassandra::Retry::Policies::Backoff }

  describe "#read_timeout" do
    it "tries again if the result did not finish, but the required acks were"
    it "tries the next host, if retries are left"

    it "stops once the max retries are reached"
    it "waits between retries"
  end

  describe "#write_timeout" do
    it "tries the next host if no acks were recieved (there is an undelying assumption that the query is idempotent)"
    it "retries if the query was a logged batch"

    it "stops once the max retries are reached"
    it "waits between retries"
  end

  describe "#unavailable" do
    it "tries the next host"

    it "stops once the max retries are reached"
    it "waits between retries"
  end

  describe "#calculate_backoff_wait_time" do
    it "allows for exponential backoffs"
    it "allows for linear backoffs"
  end
end
