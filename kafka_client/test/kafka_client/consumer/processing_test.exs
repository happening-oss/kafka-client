defmodule KafkaClient.Consumer.ProcessingTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "sequential processing on a single topic-partition" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    produced1 = produce(topic, partition: 0)
    produced2 = produce(topic, partition: 0)

    record1 = assert_processing(topic, 0)
    assert record1.offset == produced1.offset
    assert record1.payload == produced1.payload

    refute_processing(topic, 0)

    resume_processing(record1)

    record2 = assert_processing(topic, 0)
    assert record2.offset == produced2.offset
    assert record2.payload == produced2.payload
  end

  test "concurrent processing" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.topics

    produce(topic1, partition: 0)
    produce(topic1, partition: 1)
    produce(topic2, partition: 0)

    assert_processing(topic1, 0)
    assert_processing(topic1, 1)
    assert_processing(topic2, 0)
  end

  test "processing after the queue is drained" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    produce(topic, partition: 0)
    produce(topic, partition: 0)
    produce(topic, partition: 0)

    process_next_record!(topic, 0)
    process_next_record!(topic, 0)
    process_next_record!(topic, 0)

    assert buffers(consumer) == %{}

    produce(topic, partition: 0)
    process_next_record!(topic, 0)
  end
end