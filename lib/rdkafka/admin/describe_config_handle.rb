# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeConfigHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :resource_type, :int,
             :resource_name, :pointer,
             :resource, :pointer

      # @return [String] the name of the operation
      def operation_name
        "describe config"
      end

      # @return [Boolean] whether the create topic was successful
      def create_result
        DescribeConfigReport.new(self[:error_string], self[:resource_type], self[:resource_name])
      end

      def raise_error
        raise RdkafkaError.new(
          self[:response],
          broker_message: create_result.error_string
        )
      end
    end
  end
end
