defmodule KafkaClient.Consumer.SequentialTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "sequential processing on a single topic-partitions" do
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
end
