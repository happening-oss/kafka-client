defmodule KafkaClient.Consumer.MinimumMessageSizeBackpressureTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "topic-partition is not paused if the buffer is empty" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    payload = <<0::1_000_000-unit(8)>>

    %{offset: offset1} = produce(topic, partition: 0, payload: payload)
    assert_poll(topic, 0, offset1)

    %{offset: offset2} = produce(topic, partition: 0, payload: payload)
    assert_poll(topic, 0, offset2)
  end
end
