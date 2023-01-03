# frozen_string_literal: true

require "objspace"

module Rdkafka
  class Admin
    # @private
    def initialize(native_kafka)
      @native_kafka = native_kafka

      # Makes sure, that native kafka gets closed before it gets GCed by Ruby
      ObjectSpace.define_finalizer(self, native_kafka.finalizer)
    end

    def finalizer
      ->(_) { close }
    end

    # Close this admin instance
    def close
      return if closed?
      ObjectSpace.undefine_finalizer(self)
      @native_kafka.close
    end

    # Whether this admin has closed
    def closed?
      @native_kafka.closed?
    end

    # Create a topic with the given partition count and replication factor
    #
    # @raise [ConfigError] When the partition count or replication factor are out of valid range
    # @raise [RdkafkaError] When the topic name is invalid or the topic already exists
    # @raise [RdkafkaError] When the topic configuration is invalid
    #
    # @return [CreateTopicHandle] Create topic handle that can be used to wait for the result of creating the topic
    def create_topic(topic_name, partition_count, replication_factor, topic_config={})
      closed_admin_check(__method__)

      # Create a rd_kafka_NewTopic_t representing the new topic
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      new_topic_ptr = Rdkafka::Bindings.rd_kafka_NewTopic_new(
        FFI::MemoryPointer.from_string(topic_name),
        partition_count,
        replication_factor,
        error_buffer,
        256
      )
      if new_topic_ptr.null?
        raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
      end

      unless topic_config.nil?
        topic_config.each do |key, value|
          Rdkafka::Bindings.rd_kafka_NewTopic_set_config(
            new_topic_ptr,
            key.to_s,
            value.to_s
          )
        end
      end

      # Note that rd_kafka_CreateTopics can create more than one topic at a time
      pointer_array = [new_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      create_topic_handle = CreateTopicHandle.new
      create_topic_handle[:pending] = true
      create_topic_handle[:response] = -1
      CreateTopicHandle.register(create_topic_handle)
      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATETOPICS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, create_topic_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_CreateTopics(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        CreateTopicHandle.remove(create_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_NewTopic_destroy(new_topic_ptr)
      end

      create_topic_handle
    end

    # Delete the named topic
    #
    # @raise [RdkafkaError] When the topic name is invalid or the topic does not exist
    #
    # @return [DeleteTopicHandle] Delete topic handle that can be used to wait for the result of deleting the topic
    def delete_topic(topic_name)
      closed_admin_check(__method__)

      # Create a rd_kafka_DeleteTopic_t representing the topic to be deleted
      delete_topic_ptr = Rdkafka::Bindings.rd_kafka_DeleteTopic_new(FFI::MemoryPointer.from_string(topic_name))

      # Note that rd_kafka_DeleteTopics can create more than one topic at a time
      pointer_array = [delete_topic_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      delete_topic_handle = DeleteTopicHandle.new
      delete_topic_handle[:pending] = true
      delete_topic_handle[:response] = -1
      DeleteTopicHandle.register(delete_topic_handle)
      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DELETETOPICS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, delete_topic_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DeleteTopics(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DeleteTopicHandle.remove(delete_topic_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_DeleteTopic_destroy(delete_topic_ptr)
      end

      delete_topic_handle
    end

    # Incrementally alter the configuration for a specific topic
    #
    # @raise [RdkafkaError] When the topic name or configuration is invalid
    #
    # @return [AlterConfigHandle] handle that can be used to wait for the result
    def incremental_alter_config(topic_name, topic_config)
      describe_config_handle = describe_config(topic_name)
      describe_config_handle.wait

      config_resource_ptr = describe_config_handle[:resource]

      topic_config.each do |key, value|
        Rdkafka::Bindings.rd_kafka_ConfigResource_set_config(
          config_resource_ptr,
          key.to_s,
          value.to_s
        )
      end

      # Note that rd_kafka_DescribeConfigs can retrieve multiple configs
      pointer_array = [config_resource_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy(config_resource_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      alter_config_handle = AlterConfigHandle.new
      alter_config_handle[:pending] = true
      alter_config_handle[:response] = -1
      AlterConfigHandle.register(alter_config_handle)

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_ALTERCONFIGS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, alter_config_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_AlterConfigs(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DescribeConfigHandle.remove(describe_config_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy(config_resource_ptr)
      end

      describe_config_handle
    end

    private

    def closed_admin_check(method)
      raise Rdkafka::ClosedAdminError.new(method) if closed?
    end

    def describe_config(topic_name)
      closed_admin_check(__method__)

      # Create a configuration resource representing the topic
      config_resource_ptr = Rdkafka::Bindings.rd_kafka_ConfigResource_new(
        Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
        FFI::MemoryPointer.from_string(topic_name)
      )

      # Note that rd_kafka_DescribeConfigs can retrieve multiple configs
      pointer_array = [config_resource_ptr]
      topics_array_ptr = FFI::MemoryPointer.new(:pointer)
      topics_array_ptr.write_array_of_pointer(pointer_array)

      # Get a pointer to the queue that our request will be enqueued on
      queue_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_background(inner)
      end
      if queue_ptr.null?
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy(config_resource_ptr)
        raise Rdkafka::Config::ConfigError.new("rd_kafka_queue_get_background was NULL")
      end

      # Create and register the handle we will return to the caller
      describe_config_handle = DescribeConfigHandle.new
      describe_config_handle[:pending] = true
      describe_config_handle[:response] = -1
      DescribeConfigHandle.register(describe_config_handle)

      admin_options_ptr = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_AdminOptions_new(inner, Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS)
      end
      Rdkafka::Bindings.rd_kafka_AdminOptions_set_opaque(admin_options_ptr, describe_config_handle.to_ptr)

      begin
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_DescribeConfigs(
            inner,
            topics_array_ptr,
            1,
            admin_options_ptr,
            queue_ptr
          )
        end
      rescue Exception
        DescribeConfigHandle.remove(describe_config_handle.to_ptr.address)
        raise
      ensure
        Rdkafka::Bindings.rd_kafka_AdminOptions_destroy(admin_options_ptr)
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
        Rdkafka::Bindings.rd_kafka_ConfigResource_destroy(config_resource_ptr)
      end

      describe_config_handle
    end
  end
end
