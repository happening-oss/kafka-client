defmodule KafkaClient.Consumer.BackpressureTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "buffer length based pause" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.topics

    %{offset: last_buffered_offset} =
      Stream.repeatedly(fn -> produce(topic1, partition: 0) end)
      |> Stream.take(1000)
      |> Enum.at(-1)

    assert_polled(topic1, 0, last_buffered_offset)

    %{offset: first_paused_offset} = produce(topic1, partition: 0)
    refute_polled(topic1, 0, first_paused_offset)

    # check that records on other topic-partitions are still consumed
    produce(topic1, partition: 1)
    assert_processing(topic1, 1)

    produce(topic2, partition: 0)
    assert_processing(topic2, 0)

    # check that buffer is not unpaused immediately
    Stream.repeatedly(fn -> process_next_record!(topic1, 0) end) |> Enum.take(499)
    refute_polled(topic1, 0, first_paused_offset)

    # check that topic-partition is resumed after one more record is processed
    process_next_record!(topic1, 0)
    assert_polled(topic1, 0, first_paused_offset)
  end

  test "messages size based pause" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.topics

    payload = <<0::200_000-unit(8)>>

    %{offset: last_buffered_offset} =
      Stream.repeatedly(fn -> produce(topic1, partition: 0, payload: payload) end)
      |> Stream.take(5)
      |> Enum.at(-1)

    assert_polled(topic1, 0, last_buffered_offset)

    %{offset: first_paused_offset} = produce(topic1, partition: 0)
    refute_polled(topic1, 0, first_paused_offset)

    # check that records on other topic-partitions are still consumed
    produce(topic1, partition: 1)
    assert_processing(topic1, 1)

    produce(topic2, partition: 0)
    assert_processing(topic2, 0)

    # check that buffer is not unpaused immediately
    Stream.repeatedly(fn -> process_next_record!(topic1, 0) end) |> Enum.take(2)
    refute_polled(topic1, 0, first_paused_offset)

    # check that topic-partition is resumed after one more record is processed
    process_next_record!(topic1, 0)
    assert_polled(topic1, 0, first_paused_offset)
  end

  test "topic-partition is not paused if the buffer is empty" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    payload = <<0::1_000_000-unit(8)>>

    %{offset: offset1} = produce(topic, partition: 0, payload: payload)
    assert_polled(topic, 0, offset1)

    %{offset: offset2} = produce(topic, partition: 0, payload: payload)
    assert_polled(topic, 0, offset2)
  end
end
