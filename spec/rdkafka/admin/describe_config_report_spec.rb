# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::DescribeConfigReport do
  subject(:report) do
    described_class.new(
      FFI::MemoryPointer.from_string("error string"),
      Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
      FFI::MemoryPointer.from_string("topic_name")
    )
  end

  it "should get the error string" do
    expect(report.error_string).to eq("error string")
  end

  it "should get the resource name" do
    expect(report.resource_name).to eq("topic_name")
  end

  it "should get the resource type" do
    expect(report.resource_type).to eq(:topic)
  end
end
