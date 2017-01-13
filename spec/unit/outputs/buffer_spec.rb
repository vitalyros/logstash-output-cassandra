# encoding: utf-8
require "logstash/outputs/cassandra/buffer"
require "cabin"

class TestException < Exception; end

describe LogStash::Outputs::Cassandra::Buffer do
  class OperationTarget # Used to track buffer flushesn

    attr_reader :submission, :buffer_history, :receive_count
    def initialize(error = nil)
      @submission = nil
      @buffer_history = []
      @receive_count = 0
      @error = error
    end

    def receive(submission)
      @receive_count += 1
      @buffer_history << submission.clone
      @submission = submission
      if @error != nil
        raise error
      end
    end
  end

  let(:logger) { Cabin::Channel.get }
  let(:max_size) { 10 }
  let(:flush_interval) { 1 }
  # Used to track flush count
  let(:operation_target) { OperationTarget.new() }
  let(:operation) { proc {|buffer| operation_target.receive(buffer) } }
  subject(:buffer){ LogStash::Outputs::Cassandra::Buffer.new(logger, max_size, flush_interval, &operation) }
  let(:items) { (1..item_count).map { |i| i.to_s } }
  let(:item_count) { 0 }

  after(:each) do
    buffer.stop(do_flush=false)
  end
  before do
    items.each { |i| buffer << i }
  end
  it "should initialize cleanly" do
    expect(buffer).to be_a(LogStash::Outputs::Cassandra::Buffer)
  end

  describe "flushing on full" do
    context "when the buffer is filled to the max size" do
      let(:item_count) { max_size }
      it "should submit the buffer contents" do
        expect(operation_target.receive_count).to eq(1)
        expect(operation_target.submission).to eq(items)
      end
      it "should become empty" do
        contents = buffer.contents
        expect(contents.length).to eq(0)
      end

      context "when submission fails with an error" do
        let(:operation_target) {
          OperationTarget.new(TestException.new)
        }
        it "should submit the buffer contents" do
          expect(operation_target.receive_count).to eq(1)
          expect(operation_target.submission).to eq(items)
        end
        it "should not become empty" do
          expect(buffer.contents).to eq(items)
        end
      end
    end

    context "when the buffer is not filled to the max size" do
      let(:item_count) { max_size - 1 }
      it "should not submit the buffer contents" do
        expect(operation_target.receive_count).to eq(0)
      end
      it "should not become empty" do
        expect(buffer.contents).to eq(items)
      end
    end


  end

  describe "flushing by interval" do
    context "when the buffer containes a few items" do
      let(:item_count) { 3 }
      context "when the interval timer fires" do
        before do
          sleep flush_interval + 1
        end
        it "should submit the buffer contents" do
          expect(operation_target.receive_count).to eq(1)
          expect(operation_target.submission).to eq(items)
        end
        it "should become empty" do
          contents = buffer.contents
          expect(contents.length).to eq(0)
        end
      end
    end

    context "when the buffer is empty" do
      context "when the interval timer fires" do
        before do
          sleep flush_interval + 1
        end
        it "should not submit the buffer contents" do
          expect(operation_target.receive_count).to eq(0)
        end
        it "should remain empty" do
          contents = buffer.contents
          expect(contents.length).to eq(0)
        end
      end
    end
  end

  describe "flushing by stopping the buffer" do
    context "when the buffer containes a few items" do
      let(:item_count) { 3 }
      context "and the buffer is stopped with flushing" do
        before do
          buffer.stop(do_flush=true)
        end
        it "should submit the buffer contents" do
          expect(operation_target.receive_count).to eq(1)
          expect(operation_target.submission).to eq(items)
        end
        it "should become empty" do
          contents = buffer.contents
          expect(contents.length).to eq(0)
        end
      end
    end
    context "when the buffer is empty" do
      context "and the buffer is stopped with flushing" do
        before do
          buffer.stop(do_flush=true)
        end
        it "should not submit the buffer contents" do
          expect(operation_target.receive_count).to eq(0)
        end
        it "should remain empty" do
          contents = buffer.contents
          expect(contents.length).to eq(0)
        end
      end
    end
  end
end
