# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::DescribeConfigHandle do
  let(:response) { 0 }

  subject do
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:error_string] = FFI::Pointer::NULL
      handle[:resource_type] = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
      handle[:resource_name] = FFI::MemoryPointer.from_string("topic_name")
      handle[:resource] = Rdkafka::Bindings.rd_kafka_ConfigResource_new(
        handle[:resource_type], handle[:resource_name]
      )
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error described_class::WaitTimeoutError, /describe config/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a describe config report" do
        report = subject.wait

        expect(report.error_string).to eq(nil)
        expect(report.resource_name).to eq("topic_name")
        expect(report.resource_type).to eq(:topic)
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.error_string).to eq(nil)
        expect(report.resource_name).to eq("topic_name")
        expect(report.resource_type).to eq(:topic)
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    it "should raise the appropriate error" do
      expect {
        subject.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
