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
    describe "retry limit not reached" do
      it "decides to try again with the same consistency level" do
        sut_instance = sut.new(linear_backoff)

        decision = sut_instance.retry_with_backoff({ :retries => 0, :consistency => :one })

        expect(decision).to(be_an_instance_of(::Cassandra::Retry::Decisions::Retry))
        expect(decision.consistency).to(be(:one))
      end

      it "waits _before_ retrying" do
        sut_instance = sut.new(linear_backoff)
        expect(Kernel).to(receive(:sleep))

        sut_instance.retry_with_backoff({ :retries => 0 })
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

    describe "retry limit reached" do
      it "decides to reraise" do
        sut_instance = sut.new(linear_backoff)

        decision = sut_instance.retry_with_backoff({ :retries => linear_backoff["retry_limit"] + 1 })

        expect(decision).to(be_an_instance_of(::Cassandra::Retry::Decisions::Reraise))
      end

      it "does not wait" do
        sut_instance = sut.new(linear_backoff)

        expect(Kernel).not_to(receive(:sleep))

        sut_instance.retry_with_backoff({ :retries => linear_backoff["retry_limit"] + 1 })
      end
    end
  end

  describe "#read_timeout" do
    it "properly calls #retry_with_backoff" do
      sut_instance = sut.new(linear_backoff)
      expect(sut_instance).to(receive(:retry_with_backoff).with({
        :statement => "statement", :consistency => :one, :required => 1,
        :received => 0, :retrieved => false, :retries => 0
      }))

      sut_instance.read_timeout("statement", :one, 1, 0, false, 0)
    end

    it "returns the decision it got" do
      sut_instance = sut.new(linear_backoff)
      expected_result = double()
      expect(sut_instance).to(receive(:retry_with_backoff).and_return(expected_result))

      result = sut_instance.read_timeout("statement", :one, 1, 0, false, 0)

      expect(result).to(be(expected_result))
    end
  end

  describe "#write_timeout" do
    it "tries the next host if no acks were recieved (there is an undelying assumption that the query is idempotent)"
    it "retries if the query was a logged batch"
  end

  describe "#unavailable" do
    it "tries the next host"
  end
end
