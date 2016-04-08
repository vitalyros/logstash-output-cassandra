# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/backoff_retry_policy"

RSpec.describe ::Cassandra::Retry::Policies::Backoff do
  let(:sut) { ::Cassandra::Retry::Policies::Backoff }
  let(:default_options) {
    logger = double()
    allow(logger).to(receive(:error))
    {
      "logger" => logger,
      "backoff_type" => "*",
      "backoff_size" => 1,
      "retry_limit" => 1
    }
  }

  describe "#retry_with_backoff" do
    it "runs the block if the max retries have not been reached" do
      sut_instance = sut.new(default_options)
      yield_double = double()
      expect(yield_double).to(receive(:ola))

      sut_instance.retry_with_backoff({ :retries => 0 }) { |opts| yield_double.ola(opts) }
    end

    it "passes the options it recieves to the yield block"
    it "stops once the max retries are reached"
    it "waits between retries"
    it "allows for exponential backoffs"
    it "allows for linear backoffs"
  end

  describe "#read_timeout" do
    it "tries again if the result did not arrive, but the required acks arrived"
    it "tries the next host, if retries are left"
  end

  describe "#write_timeout" do
    it "tries the next host if no acks were recieved (there is an undelying assumption that the query is idempotent)"
    it "retries if the query was a logged batch"
  end

  describe "#unavailable" do
    it "tries the next host"
  end
end
