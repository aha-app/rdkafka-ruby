# frozen_string_literal: true

module Rdkafka
  module Callbacks

    # Extracts attributes of a rd_kafka_topic_result_t
    #
    # @private
    class TopicResult
      attr_reader :result_error, :error_string, :result_name

      def initialize(topic_result_pointer)
        @result_error = Rdkafka::Bindings.rd_kafka_topic_result_error(topic_result_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_topic_result_error_string(topic_result_pointer)
        @result_name = Rdkafka::Bindings.rd_kafka_topic_result_name(topic_result_pointer)
      end

      def self.create_topic_results_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    class ConfigResource
      attr_reader :error, :error_string, :resource_type, :resource_name, :resource_pointer

      def initialize(config_resource_pointer)
        @error = Rdkafka::Bindings.rd_kafka_ConfigResource_error(config_resource_pointer)
        @error_string = Rdkafka::Bindings.rd_kafka_ConfigResource_error_string(config_resource_pointer)
        @resource_type = Rdkafka::Bindings.rd_kafka_ConfigResource_type(config_resource_pointer)
        @resource_name = Rdkafka::Bindings.rd_kafka_ConfigResource_name(config_resource_pointer)
        @resource_pointer = config_resource_pointer
      end

      def self.create_config_resources_from_array(count, array_pointer)
        (1..count).map do |index|
          result_pointer = (array_pointer + (index - 1)).read_pointer
          new(result_pointer)
        end
      end
    end

    # Extracts attributs of a rd_kafka_
    # FFI Function used for Create Topic and Delete Topic callbacks
    BackgroundEventCallbackFunction = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, event_ptr, opaque_ptr|
      BackgroundEventCallback.call(client_ptr, event_ptr, opaque_ptr)
    end

    # @private
    class BackgroundEventCallback
      def self.call(_, event_ptr, _)
        event_type = Rdkafka::Bindings.rd_kafka_event_type(event_ptr)
        if event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT
          process_create_topic(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT
          process_delete_topic(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT
          process_describe_config(event_ptr)
        elsif event_type == Rdkafka::Bindings::RD_KAFKA_EVENT_ALTERCONFIGS_RESULT
          process_alter_config(event_ptr)
        end
      end

      private

      def self.process_create_topic(event_ptr)
        create_topics_result = Rdkafka::Bindings.rd_kafka_event_CreateTopics_result(event_ptr)

        # Get the number of create topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        create_topic_result_array = Rdkafka::Bindings.rd_kafka_CreateTopics_result_topics(create_topics_result, pointer_to_size_t)
        create_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, create_topic_result_array)
        create_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if create_topic_handle = Rdkafka::Admin::CreateTopicHandle.remove(create_topic_handle_ptr.address)
          create_topic_handle[:response] = create_topic_results[0].result_error
          create_topic_handle[:error_string] = create_topic_results[0].error_string
          create_topic_handle[:result_name] = create_topic_results[0].result_name
          create_topic_handle[:pending] = false
        end
      end

      def self.process_delete_topic(event_ptr)
        delete_topics_result = Rdkafka::Bindings.rd_kafka_event_DeleteTopics_result(event_ptr)

        # Get the number of topic results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        delete_topic_result_array = Rdkafka::Bindings.rd_kafka_DeleteTopics_result_topics(delete_topics_result, pointer_to_size_t)
        delete_topic_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, delete_topic_result_array)
        delete_topic_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if delete_topic_handle = Rdkafka::Admin::DeleteTopicHandle.remove(delete_topic_handle_ptr.address)
          delete_topic_handle[:response] = delete_topic_results[0].result_error
          delete_topic_handle[:error_string] = delete_topic_results[0].error_string
          delete_topic_handle[:result_name] = delete_topic_results[0].result_name
          delete_topic_handle[:pending] = false
        end
      end

      def self.process_describe_config(event_ptr)
        describe_configs_result = Rdkafka::Bindings.rd_kafka_event_DescribeConfigs_result(event_ptr)

        # Get the number of results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        describe_config_result_array = Rdkafka::Bindings.rd_kafka_DescribeConfigs_result_resources(describe_configs_result, pointer_to_size_t)
        describe_config_results = ConfigResource.create_config_resources_from_array(pointer_to_size_t.read_int, describe_config_result_array)
        describe_config_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if describe_config_handle = Rdkafka::Admin::DescribeConfigHandle.remove(describe_config_handle_ptr.address)
          describe_config_handle[:response] = describe_config_results[0].error
          describe_config_handle[:error_string] = describe_config_results[0].error_string
          describe_config_handle[:resource_name] = describe_config_results[0].resource_name
          describe_config_handle[:resource_type] = describe_config_results[0].resource_type
          describe_config_handle[:resource] = describe_config_results[0].resource_pointer
          describe_config_handle[:pending] = false
        end
      end

      def self.process_alter_config(event_ptr)
        alter_configs_result = Rdkafka::Bindings.rd_kafka_event_AlterConfigs_result(event_ptr)

        # Get the number of results
        pointer_to_size_t = FFI::MemoryPointer.new(:int32)
        alter_config_result_array = Rdkafka::Bindings.rd_kafka_DescribeConfigs_result_resources(alter_configs_result, pointer_to_size_t)
        alter_config_results = ConfigResource.create_config_resources_from_array(pointer_to_size_t.read_int, alter_config_result_array)
        alter_config_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

        if alter_config_handle = Rdkafka::Admin::DescribeConfigHandle.remove(alter_config_handle_ptr.address)
          alter_config_handle[:response] = alter_config_results[0].result_error
          alter_config_handle[:error_string] = alter_config_results[0].error_string
          alter_config_handle[:resource_name] = alter_config_results[0].resource_name
          alter_config_handle[:resource_type] = alter_config_results[0].resource_type
          alter_config_handle[:pending] = false
        end
      end
    end

    # FFI Function used for Message Delivery callbacks

    DeliveryCallbackFunction = FFI::Function.new(
        :void, [:pointer, :pointer, :pointer]
    ) do |client_ptr, message_ptr, opaque_ptr|
      DeliveryCallback.call(client_ptr, message_ptr, opaque_ptr)
    end

    # @private
    class DeliveryCallback
      def self.call(_, message_ptr, opaque_ptr)
        message = Rdkafka::Bindings::Message.new(message_ptr)
        delivery_handle_ptr_address = message[:_private].address
        if delivery_handle = Rdkafka::Producer::DeliveryHandle.remove(delivery_handle_ptr_address)
          topic_name = Rdkafka::Bindings.rd_kafka_topic_name(message[:rkt])

          # Update delivery handle
          delivery_handle[:response] = message[:err]
          delivery_handle[:partition] = message[:partition]
          delivery_handle[:offset] = message[:offset]
          delivery_handle[:topic_name] = FFI::MemoryPointer.from_string(topic_name)
          delivery_handle[:pending] = false

          # Call delivery callback on opaque
          if opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
            opaque.call_delivery_callback(Rdkafka::Producer::DeliveryReport.new(message[:partition], message[:offset], topic_name, message[:err]), delivery_handle)
          end
        end
      end
    end
  end
end
