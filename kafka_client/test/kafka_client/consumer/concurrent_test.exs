defmodule KafkaClient.Consumer.ConcurrentTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "concurrent processing" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.topics

    produce(topic1, partition: 0)
    produce(topic1, partition: 1)
    produce(topic2, partition: 0)

    assert_started_processing(topic1, 0)
    assert_started_processing(topic1, 1)
    assert_started_processing(topic2, 0)
  end
end
