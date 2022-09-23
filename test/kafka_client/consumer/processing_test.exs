defmodule KafkaClient.Consumer.ProcessingTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "sequential processing on a single topic-partition" do
    consumer = start_consumer!()
    [topic] = consumer.subscriptions

    produced1 = produce(topic, partition: 0)
    produced2 = produce(topic, partition: 0)

    record1 = assert_processing(topic, 0)
    assert record1.offset == produced1.offset
    assert record1.headers == produced1.headers
    assert record1.key == produced1.key
    assert record1.value == produced1.value

    refute_processing(topic, 0)

    resume_processing(record1)

    record2 = assert_processing(topic, 0)
    assert record2.offset == produced2.offset
    assert record2.headers == produced2.headers
    assert record2.key == produced2.key
    assert record2.value == produced2.value
  end

  test "concurrent processing" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.subscriptions

    produce(topic1, partition: 0)
    produce(topic1, partition: 1)
    produce(topic2, partition: 0)

    assert_processing(topic1, 0)
    assert_processing(topic1, 1)
    assert_processing(topic2, 0)
  end

  test "processed messages are committed" do
    consumer = start_consumer!()
    [topic] = consumer.subscriptions

    produce(topic, partition: 0)
    produce(topic, partition: 0)
    produce(topic, partition: 0)

    produce(topic, partition: 1)
    produce(topic, partition: 1)
    produce(topic, partition: 1)
    produce(topic, partition: 1)
    produce(topic, partition: 1)

    last_processed_record_partition_0 = process_next_record!(topic, 0)

    process_next_record!(topic, 1)
    last_processed_record_partition_1 = process_next_record!(topic, 1)

    eventually(
      fn ->
        offsets = KafkaClient.Consumer.Poller.committed_offsets(poller(consumer))

        assert Enum.sort(offsets) == [
                 {topic, 0, last_processed_record_partition_0.offset + 1},
                 {topic, 1, last_processed_record_partition_1.offset + 1}
               ]
      end,
      attempts: 20,
      delay: 500
    )
  end

  test "handler exception" do
    consumer = start_consumer!()
    [topic] = consumer.subscriptions

    produce(topic, partition: 0)
    produced2 = produce(topic, partition: 0)

    record = assert_processing(topic, 0)

    ExUnit.CaptureLog.capture_log(fn ->
      crash_processing(record, "some reason")

      record2 = assert_processing(topic, 0)
      assert record2.offset == produced2.offset
      assert record2.value == produced2.value
    end)
  end
end
