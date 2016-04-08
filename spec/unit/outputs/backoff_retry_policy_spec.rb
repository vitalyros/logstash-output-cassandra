# encoding: utf-8
require_relative "../../cassandra_spec_helper"
require "logstash/outputs/cassandra/backoff_retry_policy"

RSpec.describe ::Cassandra::Retry::Policies::Backoff do
  let(:sut) { ::Cassandra::Retry::Policies::Backoff }
  let(:linear_backoff) {
    logger = double()
    allow(logger).to(receive(:error))
    {
      "logger" => logger,
      "backoff_type" => "*",
      "backoff_size" => 5,
      "retry_limit" => 10
    }
  }
  let(:exponential_backoff) {
    linear_backoff.merge({
      "backoff_type" => "**",
      "backoff_size" => 2,
      "retry_limit" => 10
    })
  }

  describe "#retry_with_backoff" do
    it "runs the block if the max retries have not been reached" do
      sut_instance = sut.new(linear_backoff)
      yield_double = double()
      expect(yield_double).to(receive(:ola))

      sut_instance.retry_with_backoff({ :retries => 0 }) { yield_double.ola() }
    end

    it "passes the options it recieves to the yield block" do
      sut_instance = sut.new(linear_backoff)
      yield_double = double()
      expected_options = { :retries => 0 }
      expect(yield_double).to(receive(:ola).with(expected_options))

      sut_instance.retry_with_backoff(expected_options) { |opts| yield_double.ola(opts) }
    end

    it "stops once the max retries are reached" do
      sut_instance = sut.new(linear_backoff)
      yield_double = double()
      expect(yield_double).not_to(receive(:ola))

      sut_instance.retry_with_backoff({ :retries => linear_backoff["retry_limit"] + 1 }) { yield_double.ola() }
    end

    it "waits _before_ retrying" do
      sut_instance = sut.new(linear_backoff)
      expect(Kernel).to(receive(:sleep).ordered)
      yield_double = double()
      expect(yield_double).to(receive(:ola).ordered)

      sut_instance.retry_with_backoff({ :retries => 0 }) { yield_double.ola() }
    end

    it "allows for exponential backoffs" do
      sut_instance = sut.new(exponential_backoff)
      test_retry = exponential_backoff["retry_limit"] - 1
      expect(Kernel).to(receive(:sleep).with(exponential_backoff["backoff_size"] ** test_retry))

      sut_instance.retry_with_backoff({ :retries => test_retry }) {  }
    end

    it "allows for linear backoffs" do
      sut_instance = sut.new(linear_backoff)
      test_retry = exponential_backoff["retry_limit"] - 1
      expect(Kernel).to(receive(:sleep).with(linear_backoff["backoff_size"] * test_retry))

      sut_instance.retry_with_backoff({ :retries => test_retry }) {  }
    end
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
