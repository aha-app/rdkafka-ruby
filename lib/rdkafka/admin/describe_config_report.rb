# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeConfigReport
      # Any error message generated from the DescribeConfig
      # @return [String]
      attr_reader :error_string

      # The type of resource associated with this config (e.g. :topic)
      # @return [Symbol]
      attr_reader :resource_type

      # The name of the resource associated with this config (e.g. the topic
      # name) 
      # @return [String]
      attr_reader :resource_name

      def initialize(error_string, resource_type, resource_name)
        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end

        if resource_type == Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
          @resource_type = :topic
        end

        if resource_name != FFI::Pointer::NULL
          @resource_name = resource_name.read_string
        end
      end
    end
  end
end
