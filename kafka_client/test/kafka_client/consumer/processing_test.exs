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
    assert record1.value == produced1.value

    refute_processing(topic, 0)

    resume_processing(record1)

    record2 = assert_processing(topic, 0)
    assert record2.offset == produced2.offset
    assert record2.value == produced2.value
    assert record2.key == produced2.key
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

  test "processed messages are committed" do
    consumer = start_consumer!()
    [topic] = consumer.topics

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

    # wait a bit to ensure that the processed records are committed
    Process.sleep(1000)
    Port.command(port(consumer), :erlang.term_to_binary({:committed_offsets}))
    assert_receive {:committed, offsets}

    assert Enum.sort(offsets) == [
             {topic, 0, last_processed_record_partition_0.offset + 1},
             {topic, 1, last_processed_record_partition_1.offset + 1}
           ]
  end

  test "stream" do
    topic1 = new_test_topic()
    topic2 = new_test_topic()

    recreate_topics([topic1, topic2])

    produced =
      [
        produce(topic1, partition: 0),
        produce(topic1, partition: 1),
        produce(topic1, partition: 1),
        produce(topic2, partition: 0),
        produce(topic2, partition: 0),
        produce(topic2, partition: 0)
      ]
      |> Enum.map(&Map.take(&1, ~w/topic partition offset value/a))
      |> Enum.sort()

    events =
      KafkaClient.Consumer.Stream.new(servers: servers(), topics: [topic1, topic2])
      |> Stream.each(fn message ->
        with {:record, record} <- message,
             do: KafkaClient.Consumer.Poller.ack(record)
      end)
      |> Enum.take_while(&(&1 != :caught_up))

    # expected events are: assigned, produced records, and caught_up (which is already removed with
    # Enum.take_while)
    assert length(events) == length(produced) + 1

    assert {:assigned, _partitions} = hd(events)

    consumed =
      for({:record, record} <- events, do: Map.take(record, ~w/topic partition offset value/a))
      |> Enum.sort()

    assert consumed == produced
  end

  test "handler exception" do
    consumer = start_consumer!()
    [topic] = consumer.topics

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
