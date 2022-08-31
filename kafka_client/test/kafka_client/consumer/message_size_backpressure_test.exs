defmodule KafkaClient.Consumer.MessageSizeBackpressureTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "messages size based pause" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.topics

    payload = <<0::200_000-unit(8)>>

    %{offset: last_buffered_offset} =
      Stream.repeatedly(fn -> produce(topic1, partition: 0, payload: payload) end)
      |> Stream.take(5)
      |> Enum.at(-1)

    assert_poll(topic1, 0, last_buffered_offset)

    %{offset: first_paused_offset} = produce(topic1, partition: 0)
    refute_poll(topic1, 0, first_paused_offset)

    # check that records on other topic-partitions are still consumed
    produce(topic1, partition: 1)
    assert_processing(topic1, 1)

    produce(topic2, partition: 0)
    assert_processing(topic2, 0)

    # check that buffer is not unpaused immediately
    Stream.repeatedly(fn -> process_next_record!(topic1, 0) end) |> Enum.take(2)
    refute_poll(topic1, 0, first_paused_offset)

    # check that topic-partition is resumed after one more record is processed
    process_next_record!(topic1, 0)
    assert_poll(topic1, 0, first_paused_offset)
  end
end
